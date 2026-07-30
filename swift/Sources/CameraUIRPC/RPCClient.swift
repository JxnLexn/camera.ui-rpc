import Foundation

import Nats

public struct RPCClientOptions: Sendable {
  public var url: URL
  public var connId: String
  public var username: String?
  public var password: String?
  public var pinnedCertificate: URL?
  public var skipHostnameVerification: Bool
  public var requestTimeout: TimeInterval
  public var noResponderRetries: Int

  public init(
    url: URL,
    connId: String = UUID().uuidString,
    username: String? = nil,
    password: String? = nil,
    pinnedCertificate: URL? = nil,
    skipHostnameVerification: Bool = false,
    requestTimeout: TimeInterval = 15,
    noResponderRetries: Int = 3
  ) {
    self.url = url
    self.connId = connId
    self.username = username
    self.password = password
    self.pinnedCertificate = pinnedCertificate
    self.skipHostnameVerification = skipHostnameVerification
    self.requestTimeout = requestTimeout
    self.noResponderRetries = noResponderRetries
  }
}

public actor RPCClient {
  private let options: RPCClientOptions
  private var nats: NatsClient?
  private var replyTask: Task<Void, Never>?
  private var pending: [String: PendingRequest] = [:]
  private var assemblers: [String: ChunkAssembler] = [:]
  private var statusHandlers: [String: @Sendable () -> Void] = [:]
  private var iteratorSettles: [String: @Sendable () -> Void] = [:]
  private var counter: UInt64 = 0

  public init(options: RPCClientOptions) {
    self.options = options
  }

  public func connect() async throws {
    guard nats == nil else { return }

    var natsOptions = NatsClientOptions()
      .url(options.url)
      .reconnectWait(1)
      .maxReconnects(-1)
    if let username = options.username, let password = options.password {
      natsOptions = natsOptions.usernameAndPassword(username, password)
    }
    if let pinnedCertificate = options.pinnedCertificate {
      natsOptions = natsOptions.pinnedCertificate(pinnedCertificate)
    }
    if options.skipHostnameVerification {
      natsOptions = natsOptions.noHostnameVerification()
    }

    let client = natsOptions.build()
    try await client.connect()
    nats = client

    // one muxed inbox for all replies, routed by envelope id
    let replies = try await client.subscribe(subject: "rpc.reply.\(options.connId).>")
    replyTask = Task { [weak self] in
      do {
        for try await message in replies {
          await self?.handleReply(message)
        }
      } catch {
        await self?.failAllPending(error)
      }
    }
  }

  public func close() async {
    replyTask?.cancel()
    replyTask = nil
    try? await nats?.close()
    nats = nil
    failAllPending(RPCClientError.notConnected)
  }

  public func call(
    _ namespace: String,
    _ method: String,
    args: [Any?] = [],
    timeout: TimeInterval? = nil
  ) async throws -> Any? {
    try await callSubject("rpc.\(namespace).\(method)", args: args, timeout: timeout)
  }

  public func publish(_ subject: String, _ value: Any? = nil, reply: String? = nil) async throws {
    guard let nats else { throw RPCClientError.notConnected }
    try await nats.publish(RPCCodec.encodeMessage(value), subject: subject, reply: reply)
  }

  public func subscribe(_ subject: String) async throws -> AsyncThrowingStream<Any?, Error> {
    guard let nats else { throw RPCClientError.notConnected }
    let subscription = try await nats.subscribe(subject: subject)

    return AsyncThrowingStream { continuation in
      let task = Task {
        var assemblers: [String: ChunkAssembler] = [:]
        do {
          for try await message in subscription {
            guard let payload = message.payload else { continue }

            let chunkType = Self.header(message, "x-chunked-transfer")
            if chunkType == "header" {
              let chunkId = Self.header(message, "x-chunk-id")
              if let assembler = ChunkAssembler.fromHeader(payload: payload, chunkId: chunkId) {
                assemblers[assembler.id] = assembler
              }
              continue
            }
            if chunkType == "chunk" {
              guard
                let chunkId = Self.header(message, "x-chunk-id"),
                let assembler = assemblers[chunkId],
                let index = Self.header(message, "x-chunk-index").flatMap(Int.init)
              else { continue }
              if let assembled = assembler.add(index: index, data: payload) {
                assemblers.removeValue(forKey: chunkId)
                continuation.yield(try RPCCodec.decodeMessage(assembled))
              }
              continue
            }

            continuation.yield(try RPCCodec.decodeMessage(payload))
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in
        task.cancel()
        Task { try? await subscription.unsubscribe() }
      }
    }
  }

  /// Callback subscription (`rpc.cb.<id>`): registers with the remote method
  /// and yields every pushed value. Terminating the stream publishes the
  /// cancel frame so the server drops the callback.
  public func callWithCallback(
    _ namespace: String,
    _ method: String,
    args: [Any?] = [],
    timeout: TimeInterval? = nil
  ) async throws -> AsyncThrowingStream<Any?, Error> {
    let id = nextId()
    let callbackSubject = "rpc.cb.\(id)"
    let messages = try await subscribe(callbackSubject)

    var forward: Task<Void, Never>?
    let stream = AsyncThrowingStream<Any?, Error> { continuation in
      let task = Task {
        do {
          for try await message in messages {
            guard let map = message as? [String: Any?] else { continue }
            // error frames keep the stream alive, matching the wire contract
            if map["type"] as? String == "data" {
              continuation.yield(map["data"] ?? nil)
            }
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      forward = task
      continuation.onTermination = { [weak self] _ in
        task.cancel()
        Task {
          let cancelFrame: [String: Any?] = ["id": id]
          try? await self?.publish("\(callbackSubject).cancel", cancelFrame)
        }
      }
    }

    let params: [String: Any?] = [
      "__callback": true,
      "__callbackSubject": callbackSubject,
      "args": args,
    ]
    do {
      _ = try await call(namespace, method, args: [params], timeout: timeout)
    } catch {
      forward?.cancel()
      throw error
    }
    return stream
  }

  public func callStream(
    _ namespace: String,
    _ method: String,
    args: [Any?] = []
  ) -> AsyncThrowingStream<Any?, Error> {
    let subject = "rpc.\(namespace).\(method)"
    let delays: [UInt64] = [500_000_000, 1_000_000_000, 2_000_000_000]
    let maxRetries = options.noResponderRetries

    return AsyncThrowingStream { continuation in
      let task = Task {
        var attempt = 0
        while true {
          let id = self.nextId()
          let streamSubject = "stream.\(subject).\(id)"
          let noResponders = SignalFlag()

          do {
            guard let nats = self.natsClient() else { throw RPCClientError.notConnected }
            let subscription = try await nats.subscribe(subject: streamSubject)
            // the reply inbox only ever carries a 503 for stream requests:
            // wake the parked loop by completing the subscription
            self.registerStatusHandler(id) {
              noResponders.set()
              Task { try? await subscription.unsubscribe() }
            }

            let params: [String: Any?] = [
              "__stream": true,
              "__streamSubject": streamSubject,
              "args": args,
            ]
            let envelope: [String: Any?] = ["id": id, "method": "stream", "params": params]
            try await self.publish(subject, envelope, reply: "rpc.reply.\(id)")

            var yielded = false
            var ended = false
            var streamError: Error?
            var assemblers: [String: ChunkAssembler] = [:]

            for try await message in subscription {
              guard let payload = message.payload else { continue }

              var payloadData = payload
              let chunkType = Self.header(message, "x-chunked-transfer")
              if chunkType == "header" {
                let chunkId = Self.header(message, "x-chunk-id")
                if let assembler = ChunkAssembler.fromHeader(payload: payload, chunkId: chunkId) {
                  assemblers[assembler.id] = assembler
                }
                continue
              }
              if chunkType == "chunk" {
                guard
                  let chunkId = Self.header(message, "x-chunk-id"),
                  let assembler = assemblers[chunkId],
                  let index = Self.header(message, "x-chunk-index").flatMap(Int.init)
                else { continue }
                guard let assembled = assembler.add(index: index, data: payload) else { continue }
                assemblers.removeValue(forKey: chunkId)
                payloadData = assembled
              }

              guard
                let map = (try? RPCCodec.decodeMessage(payloadData)) as? [String: Any?],
                map["id"] as? String == id
              else { continue }

              switch map["type"] as? String {
              case "data":
                yielded = true
                continuation.yield(map["data"] ?? nil)
              case "end":
                ended = true
              case "error":
                streamError = RPCRemoteError(fromEnvelope: map["error"] as? [String: Any?])
                ended = true
              default:
                break
              }
              if ended { break }
            }

            self.removeStatusHandler(id)
            try? await subscription.unsubscribe()

            if noResponders.isSet && !yielded {
              attempt += 1
              if attempt >= maxRetries {
                continuation.finish(throwing: RPCClientError.noResponders(subject))
                return
              }
              try await Task.sleep(nanoseconds: delays[min(attempt - 1, delays.count - 1)])
              continue
            }
            if let streamError {
              continuation.finish(throwing: streamError)
              return
            }
            if !ended {
              // consumer stopped or the subscription died mid-stream
              let cancelFrame: [String: Any?] = ["id": id]
              try? await self.publish("\(streamSubject).cancel", cancelFrame)
            }
            continuation.finish()
            return
          } catch {
            self.removeStatusHandler(id)
            continuation.finish(throwing: error)
            return
          }
        }
      }
      continuation.onTermination = { _ in
        task.cancel()
      }
    }
  }

  public func callPullIterator(
    _ namespace: String,
    _ method: String,
    args: [Any?] = []
  ) async throws -> PullIterator {
    try await makePullIterator(
      subject: "rpc.\(namespace).\(method)",
      callbacks: [:],
      onewayMethods: nil,
      args: args,
      prefetch: false
    )
  }

  public func callPullIteratorWithCallback(
    _ namespace: String,
    _ method: String,
    callbacks: [String: PullCallback],
    onewayMethods: [String] = [],
    args: [Any?] = [],
    prefetch: Bool = false
  ) async throws -> PullIterator {
    try await makePullIterator(
      subject: "rpc.\(namespace).\(method)",
      callbacks: callbacks,
      onewayMethods: onewayMethods,
      args: args,
      prefetch: prefetch
    )
  }

  func callSubject(_ subject: String, args: [Any?], timeout: TimeInterval? = nil) async throws -> Any? {
    var attempt = 0
    var delay: UInt64 = 300_000_000

    while true {
      do {
        return try await callOnce(subject: subject, args: args, timeout: timeout ?? options.requestTimeout)
      } catch RPCClientError.noResponders {
        attempt += 1
        guard attempt < options.noResponderRetries else {
          throw RPCClientError.noResponders(subject)
        }
        try await Task.sleep(nanoseconds: delay)
        delay *= 2
      }
    }
  }

  func natsClient() -> NatsClient? {
    nats
  }

  func registerStatusHandler(_ id: String, _ handler: @escaping @Sendable () -> Void) {
    statusHandlers[id] = handler
  }

  func removeStatusHandler(_ id: String) {
    statusHandlers.removeValue(forKey: id)
  }

  func registerIteratorSettle(_ id: String, _ settle: @escaping @Sendable () -> Void) {
    iteratorSettles[id] = settle
  }

  func removeIteratorSettle(_ id: String) {
    iteratorSettles.removeValue(forKey: id)
  }

  // ids double as reply-subject suffixes: rpc.reply.<connId>.<unique>
  func nextId() -> String {
    counter += 1
    let millis = UInt64(Date().timeIntervalSince1970 * 1000)
    let random = UInt32.random(in: 0x1000...0xffff)
    return "\(options.connId).\(millis)-\(String(random, radix: 36))\(String(counter, radix: 36))"
  }

  private func makePullIterator(
    subject: String,
    callbacks: [String: PullCallback],
    onewayMethods: [String]?,
    args: [Any?],
    prefetch: Bool
  ) async throws -> PullIterator {
    guard let nats else { throw RPCClientError.notConnected }

    let iteratorId = nextId()
    let requestSubject = "_rpc.iterator.\(iteratorId).request"
    let responseSubject = "_rpc.iterator.\(iteratorId).response"
    // 503 status inbox for `next` requests, served by the muxed reply inbox:
    // iteratorId starts with connId, so this subject falls under the mux
    // wildcard. Real responses keep arriving on responseSubject.
    let statusInbox = "rpc.reply.\(iteratorId)"

    var callbackSub: NatsSubscription?
    let params: [String: Any?]
    if let onewayMethods {
      let callbackSubject = "_rpc.cb.\(iteratorId)"
      callbackSub = try await nats.subscribe(subject: callbackSubject)
      params = [
        "__pullCallback": true,
        "__iteratorId": iteratorId,
        "__callbackSubject": callbackSubject,
        "__callbackMethods": Array(callbacks.keys),
        "__onewayMethods": onewayMethods,
        "args": args,
      ]
    } else {
      params = ["__pullIterator": true, "__iteratorId": iteratorId, "args": args]
    }

    let initResult: Any?
    do {
      initResult = try await callSubject(subject, args: [params])
    } catch {
      try? await callbackSub?.unsubscribe()
      throw error
    }
    guard
      let initMap = initResult as? [String: Any?],
      initMap["iteratorId"] as? String == iteratorId
    else {
      try? await callbackSub?.unsubscribe()
      throw RPCClientError.invalidResponse("failed to initialize pull iterator")
    }

    let responses = try await subscribe(responseSubject)
    let session = PullIteratorSession(
      client: self,
      iteratorId: iteratorId,
      requestSubject: requestSubject,
      statusInbox: statusInbox,
      callbacks: callbacks,
      prefetch: prefetch,
      callbackSub: callbackSub
    )
    await session.start(responses: responses)
    registerStatusHandler(iteratorId) { [weak session] in
      Task { await session?.handleNoResponders(subject: subject) }
    }
    registerIteratorSettle(iteratorId) { [weak session] in
      Task { await session?.handleDisconnect() }
    }
    return PullIterator(session: session)
  }

  private func callOnce(subject: String, args: [Any?], timeout: TimeInterval) async throws -> Any? {
    guard let nats else { throw RPCClientError.notConnected }

    let id = nextId()
    let replySubject = "rpc.reply.\(id)"
    let envelope: [String: Any?] = ["id": id, "method": "call", "params": args]
    let payload = try RPCCodec.encodeMessage(envelope)

    return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Any?, Error>) in
      let timeoutTask = Task {
        try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        guard !Task.isCancelled else { return }
        self.settle(id: id, with: .failure(RPCClientError.timeout(subject)))
      }
      pending[id] = PendingRequest(continuation: continuation, timeout: timeoutTask)

      Task {
        do {
          try await nats.publish(payload, subject: subject, reply: replySubject)
        } catch {
          self.settle(id: id, with: .failure(error))
        }
      }
    }
  }

  private func settle(id: String, with result: Result<Any?, Error>) {
    guard let entry = pending.removeValue(forKey: id) else { return }
    entry.timeout.cancel()
    entry.continuation.resume(with: result)
  }

  private func failAllPending(_ error: Error) {
    let waiting = pending
    pending = [:]
    for entry in waiting.values {
      entry.timeout.cancel()
      entry.continuation.resume(throwing: error)
    }

    let settles = iteratorSettles
    iteratorSettles = [:]
    statusHandlers = [:]
    for settle in settles.values {
      settle()
    }
  }

  private func handleReply(_ message: NatsMessage) {
    // no-responder statuses carry no payload, route by subject suffix
    if message.status == .noResponders {
      let id = String(message.subject.dropFirst("rpc.reply.".count))
      if pending[id] != nil {
        settle(id: id, with: .failure(RPCClientError.noResponders(message.subject)))
      } else if let handler = statusHandlers[id] {
        handler()
      }
      return
    }

    guard let payload = message.payload else { return }

    let chunkType = Self.header(message, "x-chunked-transfer")
    if chunkType == "header" {
      let chunkId = Self.header(message, "x-chunk-id")
      if let assembler = ChunkAssembler.fromHeader(payload: payload, chunkId: chunkId) {
        assemblers[assembler.id] = assembler
      }
      return
    }
    if chunkType == "chunk" {
      guard
        let chunkId = Self.header(message, "x-chunk-id"),
        let assembler = assemblers[chunkId],
        let index = Self.header(message, "x-chunk-index").flatMap(Int.init)
      else { return }
      if let assembled = assembler.add(index: index, data: payload) {
        assemblers.removeValue(forKey: chunkId)
        route(payloadData: assembled)
      }
      return
    }

    route(payloadData: payload)
  }

  private func route(payloadData: Data) {
    guard
      let envelope = (try? RPCCodec.decodeMessage(payloadData)) as? [String: Any?],
      let id = envelope["id"] as? String
    else { return }

    if let errorMap = envelope["error"] as? [String: Any?] {
      settle(id: id, with: .failure(RPCRemoteError(fromEnvelope: errorMap)))
      return
    }
    settle(id: id, with: .success(envelope["result"] ?? nil))
  }

  private static func header(_ message: NatsMessage, _ name: String) -> String? {
    guard let headerName = try? NatsHeaderName(name) else { return nil }
    return message.headers?.get(headerName)?.description
  }
}

private struct PendingRequest {
  let continuation: CheckedContinuation<Any?, Error>
  let timeout: Task<Void, Never>
}

final class SignalFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var value = false

  var isSet: Bool {
    lock.lock()
    defer { lock.unlock() }
    return value
  }

  func set() {
    lock.lock()
    defer { lock.unlock() }
    value = true
  }
}
