import Foundation

public struct RPCRemoteError: Error, CustomStringConvertible {
  public let code: String
  public let message: String
  public let data: Any?

  public init(code: String, message: String, data: Any? = nil) {
    self.code = code
    self.message = message
    self.data = data
  }

  init(fromEnvelope envelope: [String: Any?]?) {
    self.init(
      code: envelope?["code"] as? String ?? "UNKNOWN",
      message: envelope?["message"] as? String ?? "unknown error",
      data: envelope?["data"] ?? nil
    )
  }

  public var description: String { "\(code): \(message)" }
}

public enum RPCClientError: Error, CustomStringConvertible {
  case notConnected
  case timeout(String)
  case noResponders(String)
  case invalidResponse(String)

  public var description: String {
    switch self {
    case .notConnected: return "client is not connected"
    case .timeout(let subject): return "request timed out: \(subject)"
    case .noResponders(let subject): return "no responders: \(subject)"
    case .invalidResponse(let reason): return "invalid response: \(reason)"
    }
  }
}
