import Foundation

// responses above the NATS max payload arrive as a msgpack header message
// {type:'chunked',transferId,totalChunks,totalSize,chunkSize} followed by raw
// chunks, correlated via the x-chunked-transfer / x-chunk-id / x-chunk-index
// headers (see rpc/node/src/chunking.ts)
final class ChunkAssembler {
  let id: String

  private var buffer: Data
  private var received: Set<Int> = []
  private let totalChunks: Int
  private let chunkSize: Int

  init(id: String, totalSize: Int, totalChunks: Int, chunkSize: Int?) {
    self.id = id
    self.totalChunks = totalChunks
    self.chunkSize = chunkSize ?? Int((Double(totalSize) / Double(max(totalChunks, 1))).rounded(.up))
    self.buffer = Data(count: totalSize)
  }

  static func fromHeader(payload: Data, chunkId: String?) -> ChunkAssembler? {
    guard
      let envelope = (try? RPCCodec.decode(payload)) as? [String: Any?],
      let transferId = envelope["transferId"] as? String,
      transferId == chunkId,
      let totalChunks = envelope["totalChunks"] as? Int,
      let totalSize = envelope["totalSize"] as? Int
    else { return nil }
    return ChunkAssembler(
      id: transferId,
      totalSize: totalSize,
      totalChunks: totalChunks,
      chunkSize: envelope["chunkSize"] as? Int
    )
  }

  func add(index: Int, data: Data) -> Data? {
    let offset = index * chunkSize
    guard offset + data.count <= buffer.count else { return nil }
    buffer.replaceSubrange(offset..<(offset + data.count), with: data)
    received.insert(index)
    return received.count == totalChunks ? buffer : nil
  }
}
