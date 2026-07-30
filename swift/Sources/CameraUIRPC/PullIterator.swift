import Foundation

import Nats

public typealias PullCallback = @Sendable ([Any?]) async -> Void

public final class PullIterator: AsyncSequence, Sendable {
  public typealias Element = Any?

  let session: PullIteratorSession

  init(session: PullIteratorSession) {
    self.session = session
  }

  deinit {
    let session = self.session
    Task { await session.cancel() }
  }

  public func makeAsyncIterator() -> Iterator {
    Iterator(session: session)
  }

  public func cancel() async {
    await session.cancel()
  }

  public struct Iterator: AsyncIteratorProtocol {
    let session: PullIteratorSession

    public func next() async throws -> Any?? {
      try await session.next()
    }
  }
}

actor PullIteratorSession {
  private let client: RPCClient
  private let iteratorId: String
  private let requestSubject: String
  private let statusInbox: String
  private let callbacks: [String: PullCallback]
  private let prefetch: Bool
  private let callbackSub: NatsSubscription?
  private var responseTask: Task<Void, Never>?
  private var queue: [[String: Any?]] = []
  private var waiter: CheckedContinuation<[String: Any?], Never>?
  private var ended = false
  private var endedError: Error?
  private var prefetched = false
  private var cleanedUp = false

  init(
    client: RPCClient,
    iteratorId: String,
    requestSubject: String,
    statusInbox: String,
    callbacks: [String: PullCallback],
    prefetch: Bool,
    callbackSub: NatsSubscription?
  ) {
    self.client = client
    self.iteratorId = iteratorId
    self.requestSubject = requestSubject
    self.statusInbox = statusInbox
    self.callbacks = callbacks
    self.prefetch = prefetch
    self.callbackSub = callbackSub
  }

  func start(responses: AsyncThrowingStream<Any?, Error>) {
    responseTask = Task { [weak self] in
      do {
        for try await message in responses {
          guard let map = message as? [String: Any?] else { continue }
          await self?.onResponse(map)
        }
      } catch {
        await self?.onResponse([
          "type": "error",
          "error": ["code": "SUBSCRIPTION", "message": "\(error)"] as [String: Any?],
        ])
      }
    }
  }

  func next() async throws -> Any?? {
    if cleanedUp { return nil }
    if ended {
      await cleanup(sendCancel: false)
      if let error = endedError {
        endedError = nil
        throw error
      }
      return nil
    }

    if prefetched {
      prefetched = false
    } else {
      do {
        try await sendNextRequest()
      } catch {
        await cleanup(sendCancel: true)
        throw error
      }
    }

    let response = await withTaskCancellationHandler {
      await awaitResponse()
    } onCancel: {
      Task { await self.cancel() }
    }

    switch response["type"] as? String {
    case "value":
      // opt-in n+1 prefetch: request the next batch before draining this
      // one, so the server produces while the client processes
      if prefetch && !ended && !cleanedUp {
        do {
          try await sendNextRequest()
          prefetched = true
        } catch {
          prefetched = false
        }
      }
      await drainCallbacks()
      return .some(response["value"] ?? nil)
    case "done":
      await drainCallbacks()
      await cleanup(sendCancel: false)
      return nil
    case "cancelled":
      return nil
    default:
      await cleanup(sendCancel: false)
      if let error = endedError {
        endedError = nil
        throw error
      }
      throw RPCRemoteError(fromEnvelope: response["error"] as? [String: Any?])
    }
  }

  func cancel() async {
    if cleanedUp { return }
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: ["type": "cancelled"])
    }
    await cleanup(sendCancel: true)
  }

  func handleNoResponders(subject: String) {
    onResponse([
      "type": "error",
      "error": ["code": "503", "message": "No responders for \(subject)"] as [String: Any?],
    ])
  }

  func handleDisconnect() {
    if !ended {
      ended = true
      endedError = RPCClientError.notConnected
    }
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: ["type": "disconnected"])
    }
  }

  private func onResponse(_ map: [String: Any?]) {
    switch map["type"] as? String {
    case "error":
      ended = true
      endedError = RPCRemoteError(fromEnvelope: map["error"] as? [String: Any?])
    case "done":
      ended = true
    default:
      break
    }
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: map)
    } else {
      queue.append(map)
    }
  }

  private func sendNextRequest() async throws {
    let request: [String: Any?] = ["id": iteratorId, "type": "next"]
    try await client.publish(requestSubject, request, reply: statusInbox)
  }

  private func awaitResponse() async -> [String: Any?] {
    if !queue.isEmpty { return queue.removeFirst() }
    if ended { return ["type": endedError == nil ? "done" : "error"] }
    return await withCheckedContinuation { waiter = $0 }
  }

  // callback messages of a batch arrive before the boundary response (same
  // connection, publish order), so they are already buffered here: a
  // non-blocking drain sees the whole batch. Handlers run serially — a slow
  // handler stalls the next request, which parks the server at its yield.
  private func drainCallbacks() async {
    guard let callbackSub else { return }
    while true {
      guard let message = (try? callbackSub.tryNextMessage()) ?? nil else { break }
      guard
        let payload = message.payload,
        let invocation = (try? RPCCodec.decodeMessage(payload)) as? [String: Any?],
        let method = invocation["method"] as? String,
        let handler = callbacks[method]
      else { continue }
      await handler(invocation["args"] as? [Any?] ?? [])
    }
  }

  private func cleanup(sendCancel: Bool) async {
    if cleanedUp { return }
    cleanedUp = true
    responseTask?.cancel()
    responseTask = nil
    await client.removeStatusHandler(iteratorId)
    await client.removeIteratorSettle(iteratorId)
    try? await callbackSub?.unsubscribe()
    if sendCancel && !ended {
      ended = true
      let request: [String: Any?] = ["id": iteratorId, "type": "cancel"]
      try? await client.publish(requestSubject, request)
    }
    ended = true
  }
}
