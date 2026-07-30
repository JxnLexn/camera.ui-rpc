import XCTest

@testable import CameraUIRPC

final class CodecTests: XCTestCase {
  func testScalarRoundtrips() throws {
    let values: [Any?] = [
      nil, true, false, 0, 42, -42, 127, 128, -32, -33, 65535, -65536,
      2_147_483_647, -2_147_483_648, 9_007_199_254_740_991, 3.14159, -2.5,
      "", "hello", "你好 🌍", Data(), Data([1, 2, 3]),
    ]
    for value in values {
      let decoded = try MessagePack.decode(try MessagePack.encode(value))
      XCTAssertTrue(equal(value, decoded), "roundtrip mismatch for \(String(describing: value))")
    }
  }

  func testTimestampFormats() throws {
    // ts32 (whole seconds), ts64 (with nanos), ts96 (pre-epoch)
    for date in [
      Date(timeIntervalSince1970: 1_708_776_000),
      Date(timeIntervalSince1970: 1_708_776_000.123),
      Date(timeIntervalSince1970: -86_400),
    ] {
      let decoded = try MessagePack.decode(try MessagePack.encode(date)) as? Date
      XCTAssertNotNil(decoded)
      XCTAssertEqual(decoded!.timeIntervalSince1970, date.timeIntervalSince1970, accuracy: 0.001)
    }
  }

  func testUndefinedExtDecodesToNil() throws {
    // msgpackr encodes JS undefined as fixext1 type 0
    let data = Data([0xd4, 0x00, 0x00])
    let decoded = try MessagePack.decode(data)
    XCTAssertNil(decoded)
  }

  func testMapWithNilValueKeepsKey() throws {
    let decoded = try MessagePack.decode(try MessagePack.encode(["a": nil, "b": 1] as [String: Any?])) as? [String: Any?]
    XCTAssertEqual(decoded?.count, 2)
    XCTAssertNotNil(decoded?.index(forKey: "a"))
  }

  func testSmallBinaryStaysInline() throws {
    let message: [String: Any?] = ["frame": Data(repeating: 7, count: 100)]
    let encoded = try RPCCodec.encodeMessage(message)
    XCTAssertNotEqual(Array(encoded.prefix(4)), RPCCodec.magic)
    let decoded = try RPCCodec.decodeMessage(encoded) as? [String: Any?]
    XCTAssertEqual(decoded?["frame"] as? Data, Data(repeating: 7, count: 100))
  }

  func testLargeBinaryUsesFrame() throws {
    let blob0 = Data(repeating: 1, count: RPCCodec.binaryExtractThreshold)
    let blob1 = Data(repeating: 2, count: RPCCodec.binaryExtractThreshold + 5)
    let message: [String: Any?] = [
      "a": ["frame": blob0] as [String: Any?],
      "b": [blob1] as [Any?],
      "meta": "x",
    ]
    let encoded = try RPCCodec.encodeMessage(message)
    XCTAssertEqual(Array(encoded.prefix(4)), RPCCodec.magic)

    let decoded = try RPCCodec.decodeMessage(encoded) as? [String: Any?]
    XCTAssertEqual((decoded?["a"] as? [String: Any?])?["frame"] as? Data, blob0)
    XCTAssertEqual((decoded?["b"] as? [Any?])?.first as? Data, blob1)
    XCTAssertEqual(decoded?["meta"] as? String, "x")
  }

  func testOutOfOrderPlaceholders() throws {
    // hand-build a frame whose placeholders appear in reverse traversal order
    let blob0 = Data(repeating: 0xaa, count: 16)
    let blob1 = Data(repeating: 0xbb, count: 24)
    let envelope: [String: Any?] = [
      "z_second": [RPCCodec.placeholderKey: 1, "l": blob1.count] as [String: Any?],
      "a_first": [RPCCodec.placeholderKey: 0, "l": blob0.count] as [String: Any?],
    ]
    let envelopeData = try MessagePack.encode(envelope)
    var frame = Data(RPCCodec.magic)
    let envLen = envelopeData.count
    frame.append(contentsOf: [
      UInt8(envLen & 0xff), UInt8((envLen >> 8) & 0xff), UInt8((envLen >> 16) & 0xff), UInt8((envLen >> 24) & 0xff),
    ])
    frame.append(envelopeData)
    frame.append(blob0)
    frame.append(blob1)

    let decoded = try RPCCodec.decodeMessage(frame) as? [String: Any?]
    XCTAssertEqual(decoded?["a_first"] as? Data, blob0)
    XCTAssertEqual(decoded?["z_second"] as? Data, blob1)
  }

  func testUserMapWithPlaceholderKeyPassesThrough() throws {
    let message: [String: Any?] = [
      "fake": [RPCCodec.placeholderKey: 0, "l": 5, "extra": true] as [String: Any?],
      "blob": Data(repeating: 3, count: RPCCodec.binaryExtractThreshold),
    ]
    let decoded = try RPCCodec.decodeMessage(try RPCCodec.encodeMessage(message)) as? [String: Any?]
    XCTAssertEqual((decoded?["fake"] as? [String: Any?])?["extra"] as? Bool, true)
    XCTAssertEqual(decoded?["blob"] as? Data, Data(repeating: 3, count: RPCCodec.binaryExtractThreshold))
  }

  func testTruncatedFrameThrows() throws {
    let blob = Data(repeating: 1, count: RPCCodec.binaryExtractThreshold)
    var encoded = try RPCCodec.encodeMessage(["blob": blob] as [String: Any?])
    encoded.removeLast(10)
    XCTAssertThrowsError(try RPCCodec.decodeMessage(encoded))
  }

  private func equal(_ a: Any?, _ b: Any?) -> Bool {
    switch (a, b) {
    case (nil, nil): return true
    case (let x as Bool, let y as Bool): return x == y
    case (let x as String, let y as String): return x == y
    case (let x as Data, let y as Data): return x == y
    case (let x as Int, let y as Int): return x == y
    case (let x as Double, let y as Double): return x == y
    default: return false
    }
  }
}
