using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using LZ4;
using ZstdNet;

namespace Sandbox
{
	/// <summary>
	/// Lightweight BinaryKV3 reader sufficient for HL:A DATA/MDAT skeletons.
	/// Supports KV3 versions 1-5, compression 0 (none), 1 (LZ4), 2 (ZSTD).
	/// Builds a simple object graph (Dictionary/List/primitive).
	/// </summary>
	internal static class BinaryKV3Lite
	{
		private const uint MAGIC0 = 0x03564B56; // VKV3 (0x03 'V''K''V')
		private const uint MAGIC_MASK = 0xFFFFFF00;
		private const uint MAGIC_KV3 = 0x4B563300; // 'KV3\x00'

		private enum NodeType : byte
		{
			NULL = 1,
			BOOLEAN = 2,
			INT64 = 3,
			UINT64 = 4,
			DOUBLE = 5,
			STRING = 6,
			BINARY_BLOB = 7,
			ARRAY = 8,
			OBJECT = 9,
			ARRAY_TYPED = 10,
			INT32 = 11,
			UINT32 = 12,
			BOOLEAN_TRUE = 13,
			BOOLEAN_FALSE = 14,
			INT64_ZERO = 15,
			INT64_ONE = 16,
			DOUBLE_ZERO = 17,
			DOUBLE_ONE = 18,
			FLOAT = 19,
			INT16 = 20,
			UINT16 = 21,
			UNKNOWN_22 = 22,
			INT32_AS_BYTE = 23,
			ARRAY_TYPE_BYTE_LENGTH = 24,
			ARRAY_TYPE_AUXILIARY_BUFFER = 25,
		}

		private struct Buffers
		{
			public ArraySegment<byte> Bytes1;
			public ArraySegment<byte> Bytes2;
			public ArraySegment<byte> Bytes4;
			public ArraySegment<byte> Bytes8;
		}

		private class Context
		{
			public int Version;
			public ArraySegment<byte> Types;
			public ArraySegment<byte> ObjectLengths;
			public ArraySegment<byte> BinaryBlobs;
			public ArraySegment<byte> BinaryBlobLengths;
			public string[] Strings;
			public Buffers Buffer;
			public Buffers AuxBuffer;
		}

		public static Dictionary<string, object> Parse(byte[] kv3Bytes)
		{
			using var ms = new MemoryStream(kv3Bytes, false);
			using var br = new BinaryReader(ms);

			uint magic = br.ReadUInt32();
			if (magic == MAGIC0)
			{
				// Version 0 legacy (rare). Not supported.
				throw new InvalidDataException("KV3 v0 not supported in lite reader");
			}

			int version = (int)(magic & 0xFF);
			uint sig = magic & MAGIC_MASK;
			if (sig != MAGIC_KV3 || version < 1 || version > 5)
			{
				throw new InvalidDataException($"Unsupported KV3 signature/version: 0x{magic:X8}");
			}

			var ctx = ReadBuffers(version, br);
			// Log.Info($"[KV3Lite] ReadBuffers ok v={version}");
			var (rootType, _) = ReadType(ctx);
			if (rootType == NodeType.OBJECT)
			{
				var root = ReadObject(ctx);
				// Log.Info($"[KV3Lite] Root OBJECT parsed ok");
				return root;
			}
			else if (rootType == NodeType.NULL)
			{
				// Log.Info($"[KV3Lite] Root is NULL");
				return new Dictionary<string, object>();
			}
			else if (rootType == NodeType.ARRAY || rootType == NodeType.ARRAY_TYPED || rootType == NodeType.ARRAY_TYPE_BYTE_LENGTH)
			{
				var arr = rootType == NodeType.ARRAY ? ReadArray(ctx) : (rootType == NodeType.ARRAY_TYPED ? ReadTypedArray(ctx) : ReadTypedArray(ctx, true));
				// Wrap array as { "_": [...] } to satisfy expected object
				var dict = new Dictionary<string, object>(1) { { "_", arr } };
				// Log.Info($"[KV3Lite] Root ARRAY parsed ok (len={arr.Count})");
				return dict;
			}
			else
			{
				// Log.Info($"[KV3Lite] Root type {rootType} not OBJECT; returning empty");
				return new Dictionary<string, object>();
			}
		}

		private static Context ReadBuffers(int version, BinaryReader br)
		{
			var ctx = new Context { Version = version };

			// format guid
			_ = br.ReadBytes(16);

			uint compressionMethod = br.ReadUInt32(); // 0 none, 1 lz4, 2 zstd
			ushort dictId = 0;
			ushort frameSize = 0;
			int countBytes1, countBytes4, countBytes8, countTypes = 0, countObjects = 0, countArrays = 0;
			int sizeUncompressedTotal, sizeCompressedTotal = 0, countBlocks = 0, sizeBinaryBlobsBytes = 0;

            if (version == 1)
			{
				countBytes1 = br.ReadInt32();
				countBytes4 = br.ReadInt32();
				countBytes8 = br.ReadInt32();
				sizeUncompressedTotal = br.ReadInt32();
				sizeCompressedTotal = (int)(br.BaseStream.Length - br.BaseStream.Position);
                // Log.Info($"[KV3Lite] v1 hdr cm={compressionMethod} b1={countBytes1} b4={countBytes4} b8={countBytes8} sizeU={sizeUncompressedTotal} sizeC={sizeCompressedTotal}");
			}
			else
			{
				dictId = br.ReadUInt16();
				frameSize = br.ReadUInt16();
				countBytes1 = br.ReadInt32();
				countBytes4 = br.ReadInt32();
				countBytes8 = br.ReadInt32();
				countTypes = br.ReadInt32();
				countObjects = br.ReadUInt16();
				countArrays = br.ReadUInt16();
				sizeUncompressedTotal = br.ReadInt32();
				sizeCompressedTotal = br.ReadInt32();
				countBlocks = br.ReadInt32();
				sizeBinaryBlobsBytes = br.ReadInt32();
                // Log.Info($"[KV3Lite] v{version} hdr cm={compressionMethod} dict={dictId} frame={frameSize} b1={countBytes1} b4={countBytes4} b8={countBytes8} types={countTypes} objs={countObjects} arrs={countArrays} blocks={countBlocks} sizeU={sizeUncompressedTotal} sizeC={sizeCompressedTotal} blobsU={sizeBinaryBlobsBytes}");
			}

			int countBytes2 = 0, sizeBlockCompressedSizesBytes = 0;
            if (version >= 4)
			{
				countBytes2 = br.ReadInt32();
				sizeBlockCompressedSizesBytes = br.ReadInt32();
                // Log.Info($"[KV3Lite] v{version} extra b2={countBytes2} blockSizesBytes={sizeBlockCompressedSizesBytes}");
			}

			int sizeU1 = 0, sizeC1 = 0, sizeU2 = 0, sizeC2 = 0;
			int cB1 = 0, cB2 = 0, cB4 = 0, cB8 = 0, cObj2 = 0, cArr2 = 0;
            if (version >= 5)
			{
				sizeU1 = br.ReadInt32();
				sizeC1 = br.ReadInt32();
				sizeU2 = br.ReadInt32();
				sizeC2 = br.ReadInt32();
				cB1 = br.ReadInt32();
				cB2 = br.ReadInt32();
				cB4 = br.ReadInt32();
				cB8 = br.ReadInt32();
				_ = br.ReadInt32(); // unk13
				cObj2 = br.ReadInt32();
				cArr2 = br.ReadInt32();
				_ = br.ReadInt32(); // unk16
                // Log.Info($"[KV3Lite] v5 sizes u1={sizeU1} c1={sizeC1} u2={sizeU2} c2={sizeC2} cB1={cB1} cB2={cB2} cB4={cB4} cB8={cB8} objs2={cObj2} arrs2={cArr2}");
			}
			else
			{
				sizeU1 = sizeUncompressedTotal;
				sizeC1 = sizeCompressedTotal;
			}

			var buf1Raw = ArrayPool<byte>.Shared.Rent((version < 5 && compressionMethod == 2) ? sizeU1 + sizeBinaryBlobsBytes : sizeU1);
			byte[] buf2Raw = null;
			byte[] blobsRaw = null;

			var decomp = (compressionMethod == 2) ? new Decompressor() : null;
			int offsetAfterTypesV5 = 0;
			try
			{
				var buf1Span = new ArraySegment<byte>(buf1Raw, 0, sizeU1);
				if (compressionMethod == 0)
				{
					br.Read(buf1Span);
					// Log.Info($"[KV3Lite] buf1 uncompressed read {buf1Span.Count} bytes");
				}
				else if (compressionMethod == 1)
				{
					// LZ4 framed stream broken into frames for v>=2, use provided frame size
					ReadLZ4(br, buf1Span, sizeC1, frameSize);
					// Log.Info($"[KV3Lite] buf1 LZ4 decoded {buf1Span.Count} bytes");
				}
				else if (compressionMethod == 2)
				{
					int outLen = sizeU1;
					if (version < 5) outLen += sizeBinaryBlobsBytes;
					var enc = br.ReadBytes(sizeC1);
					var outArr = new byte[outLen];
					var written = decomp.Unwrap(enc, outArr, 0, true);
					Buffer.BlockCopy(outArr, 0, buf1Raw, 0, sizeU1);
					if (version < 5)
					{
						blobsRaw = ArrayPool<byte>.Shared.Rent(sizeBinaryBlobsBytes);
						Buffer.BlockCopy(outArr, sizeU1, blobsRaw, 0, sizeBinaryBlobsBytes);
					}
					// Log.Info($"[KV3Lite] buf1 ZSTD decoded {outLen} bytes -> {sizeU1}+{sizeBinaryBlobsBytes}");
				}

				var buffer1 = new Buffers();
				int off = 0;

				if (countBytes1 > 0)
				{
					buffer1.Bytes1 = buf1Span[off..(off + countBytes1)];
					off += countBytes1;
				}
				if (countBytes2 > 0)
				{
					off = Align(off, 2);
					buffer1.Bytes2 = buf1Span[off..(off + countBytes2 * 2)];
					off += countBytes2 * 2;
				}
				if (countBytes4 > 0)
				{
					off = Align(off, 4);
					buffer1.Bytes4 = buf1Span[off..(off + countBytes4 * 4)];
					off += countBytes4 * 4;
				}
				if (countBytes8 > 0)
				{
					off = Align(off, 8);
					buffer1.Bytes8 = buf1Span[off..(off + countBytes8 * 8)];
					off += countBytes8 * 8;
				}
				else if (version < 5)
				{
					off = Align(off, 8);
				}

                int countStrings = ReadInt(ref buffer1.Bytes4);
				ctx.Strings = new string[countStrings];
                // Log.Info($"[KV3Lite] strings={countStrings}");

                if (version >= 5)
				{
					ctx.AuxBuffer = buffer1;
					int readStrBytes = 0;
					for (int i = 0; i < countStrings; i++)
					{
						ctx.Strings[i] = ReadNullTermString(ref buffer1.Bytes1, ref readStrBytes);
					}
					// exactly consumed
				}
				else
				{
                    ctx.Buffer = buffer1;
                    var stringsStartOffset = off;
                    var stringsBuffer = buf1Span[off..];
                    int readStringBytes = 0;
                    for (int i = 0; i < countStrings; i++)
                    {
                        ctx.Strings[i] = ReadNullTermString(ref stringsBuffer, ref readStringBytes);
                    }
                    off = stringsStartOffset + readStringBytes;
                    // Log.Info($"[KV3Lite] stringsReadBytes={readStringBytes} newOff={off}");
                    int typesLength = (version == 1) ? (sizeU1 - off - 4) : (countTypes - off + stringsStartOffset);
                    if (typesLength < 0) typesLength = 0;
                    ctx.Types = buf1Span[off..(off + typesLength)];
                    off += typesLength;
					if (countBlocks == 0)
					{
						_ = MemoryMarshal.Read<uint>(buf1Span[off..]);
						off += 4;
					}
                    // Log.Info($"[KV3Lite] typesLen={typesLength} off={off} blocks={countBlocks}");
				}

				if (version >= 5)
				{
					buf2Raw = ArrayPool<byte>.Shared.Rent(sizeU2);
					var buf2Span = new ArraySegment<byte>(buf2Raw, 0, sizeU2);
					if (compressionMethod == 0)
					{
						br.Read(buf2Span);
						// Log.Info($"[KV3Lite] buf2 uncompressed read {buf2Span.Count} bytes");
					}
					else if (compressionMethod == 1)
					{
						ReadLZ4(br, buf2Span, sizeC2, frameSize);
						// Log.Info($"[KV3Lite] buf2 LZ4 decoded {buf2Span.Count} bytes");
					}
					else
					{
						var enc2 = br.ReadBytes(sizeC2);
						var tmp = new byte[sizeU2];
						_ = decomp!.Unwrap(enc2, tmp, 0, true);
						Buffer.BlockCopy(tmp, 0, buf2Raw, 0, sizeU2);
						// Log.Info($"[KV3Lite] buf2 ZSTD decoded {sizeU2} bytes");
					}

					var buf2 = new Buffers();
					ctx.Buffer = buf2;
					int end = cObj2 * sizeof(int);
					ctx.ObjectLengths = buf2Span[..end];
					int o2 = end;
					if (cB1 > 0) { buf2.Bytes1 = buf2Span[o2..(o2 + cB1)]; o2 += cB1; }
					if (cB2 > 0) { o2 = Align(o2, 2); buf2.Bytes2 = buf2Span[o2..(o2 + cB2 * 2)]; o2 += cB2 * 2; }
					if (cB4 > 0) { o2 = Align(o2, 4); buf2.Bytes4 = buf2Span[o2..(o2 + cB4 * 4)]; o2 += cB4 * 4; }
					if (cB8 > 0) { o2 = Align(o2, 8); buf2.Bytes8 = buf2Span[o2..(o2 + cB8 * 8)]; o2 += cB8 * 8; }
					ctx.Types = buf2Span[o2..(o2 + countTypes)];
					offsetAfterTypesV5 = o2 + countTypes;
					// trailer/skipped if blocks exist handled below
				}

				if (countBlocks > 0)
				{
					int end = countBlocks * sizeof(int);
					ArraySegment<byte> withLengths;
					if (version >= 5)
					{
						var buf2Span = new ArraySegment<byte>(buf2Raw, 0, sizeU2);
						withLengths = buf2Span[offsetAfterTypesV5..];
					}
					else
					{
						withLengths = buf1Span[off..];
					}
					ctx.BinaryBlobLengths = withLengths[..end];
					withLengths = withLengths[end..];
					_ = MemoryMarshal.Read<uint>(withLengths);
					withLengths = withLengths[sizeof(uint)..];

					if (compressionMethod == 0)
					{
						blobsRaw = ArrayPool<byte>.Shared.Rent(sizeBinaryBlobsBytes);
						ctx.BinaryBlobs = new ArraySegment<byte>(blobsRaw, 0, sizeBinaryBlobsBytes);
						br.Read(ctx.BinaryBlobs);
						// Log.Info($"[KV3Lite] blobs uncompressed read {sizeBinaryBlobsBytes} bytes");
					}
					else if (compressionMethod == 1)
					{
						blobsRaw = ArrayPool<byte>.Shared.Rent(sizeBinaryBlobsBytes);
						ctx.BinaryBlobs = new ArraySegment<byte>(blobsRaw, 0, sizeBinaryBlobsBytes);
						ReadLZ4Blocks(br, ctx.BinaryBlobs, sizeBinaryBlobsBytes, frameSize, sizeBlockCompressedSizesBytes, withLengths);
						// Log.Info($"[KV3Lite] blobs LZ4 decoded {sizeBinaryBlobsBytes} bytes");
					}
					else if (compressionMethod == 2)
					{
						if (version >= 5)
						{
							int sizeCompressedBlobs = sizeCompressedTotal - sizeC1 - sizeC2;
							blobsRaw = ArrayPool<byte>.Shared.Rent(sizeBinaryBlobsBytes);
							ctx.BinaryBlobs = new ArraySegment<byte>(blobsRaw, 0, sizeBinaryBlobsBytes);
							var enc = br.ReadBytes(sizeCompressedBlobs);
							_ = decomp!.Unwrap(enc, blobsRaw, 0, true);
						// Log.Info($"[KV3Lite] blobs ZSTD decoded {sizeBinaryBlobsBytes} bytes (comp={sizeCompressedBlobs})");
						}
						else
						{
							ctx.BinaryBlobs = new ArraySegment<byte>(buf1Raw, sizeU1, sizeBinaryBlobsBytes);
						// Log.Info($"[KV3Lite] blobs from tail of buf1 {sizeBinaryBlobsBytes} bytes");
						}
					}
					_ = br.ReadUInt32(); // trailer
				}
			}
			finally
			{
				decomp?.Dispose();
			}

			return ctx;
		}

		private static void ReadLZ4(BinaryReader br, ArraySegment<byte> dest, int compressedSize, ushort frameSize)
		{
			var enc = br.ReadBytes(compressedSize);
			int decoded = LZ4Codec.Decode(enc, 0, enc.Length, dest.Array, dest.Offset, dest.Count, true);
			if (decoded != dest.Count)
			{
				throw new InvalidDataException($"LZ4 decode mismatch {decoded}!={dest.Count}");
			}
		}

        private static void ReadLZ4Blocks(BinaryReader br, ArraySegment<byte> outBuf, int totalLen, ushort frameSize, int sizeLengths, ArraySegment<byte> lengthsStream)
		{
			int decompressedOffset = 0;
            while (lengthsStream.Count >= sizeof(ushort))
			{
				ushort blockLen = MemoryMarshal.Read<ushort>(lengthsStream);
				lengthsStream = lengthsStream[sizeof(ushort)..];
				var enc = br.ReadBytes(blockLen);
				int outBlock = Math.Min(frameSize, totalLen - decompressedOffset);
				int decoded = LZ4Codec.Decode(enc, 0, enc.Length, outBuf.Array, outBuf.Offset + decompressedOffset, outBlock, true);
				if (decoded <= 0) throw new InvalidOperationException("LZ4 block decode failed");
				decompressedOffset += decoded;
			}
		}

		private static int Align(int offset, int alignment)
		{
			int a = alignment - 1;
			offset += a;
			return offset & ~a;
		}

		private static int ReadInt(ref ArraySegment<byte> seg)
		{
			int v = MemoryMarshal.Read<int>(seg);
			seg = seg[sizeof(int)..];
			return v;
		}

		private static string ReadNullTermString(ref ArraySegment<byte> seg, ref int offset)
		{
			var span = seg.AsSpan();
			int idx = span.IndexOf((byte)0);
			if (idx < 0) idx = span.Length;
			string s = System.Text.Encoding.UTF8.GetString(span[..idx]);
			seg = seg[(idx + 1)..];
			offset += idx + 1;
			return s;
		}

		private static Dictionary<string, object> ReadObject(Context ctx)
		{
			int length;
			if (ctx.Version >= 5)
			{
				length = MemoryMarshal.Read<int>(ctx.ObjectLengths);
				ctx.ObjectLengths = ctx.ObjectLengths[sizeof(int)..];
			}
			else
			{
				length = ReadInt(ref ctx.Buffer.Bytes4);
			}
			if (length < 0) length = 0;
			var obj = new Dictionary<string, object>(Math.Max(1, length), StringComparer.Ordinal);
			for (int i = 0; i < length; i++)
			{
				ReadValue(ctx, obj, false);
			}
			return obj;
		}

		private static void ReadValue(Context ctx, Dictionary<string, object> parent, bool inArray)
		{
			string name = null;
			if (!inArray)
			{
				int strId = ReadInt(ref ctx.Buffer.Bytes4);
				name = (strId == -1) ? string.Empty : ctx.Strings[strId];
			}
			var (type, flag) = ReadType(ctx);
			object val = type switch
			{
				NodeType.NULL => null,
				NodeType.BOOLEAN => ReadBool(ref ctx.Buffer.Bytes1),
				NodeType.BOOLEAN_TRUE => true,
				NodeType.BOOLEAN_FALSE => false,
				NodeType.INT16 => (short)ReadU16(ref ctx.Buffer.Bytes2),
				NodeType.UINT16 => ReadU16(ref ctx.Buffer.Bytes2),
				NodeType.INT32 => ReadInt(ref ctx.Buffer.Bytes4),
				NodeType.UINT32 => (uint)ReadInt(ref ctx.Buffer.Bytes4),
				NodeType.FLOAT => ReadFloat(ref ctx.Buffer.Bytes4),
				NodeType.INT64 => ReadInt64(ref ctx.Buffer.Bytes8),
				NodeType.UINT64 => ReadUInt64(ref ctx.Buffer.Bytes8),
				NodeType.DOUBLE => ReadDouble(ref ctx.Buffer.Bytes8),
				NodeType.INT64_ZERO => (long)0,
				NodeType.INT64_ONE => (long)1,
				NodeType.DOUBLE_ZERO => 0.0,
				NodeType.DOUBLE_ONE => 1.0,
				NodeType.STRING => ReadString(ctx),
				NodeType.BINARY_BLOB => ReadBinaryBlob(ctx),
				NodeType.ARRAY => ReadArray(ctx),
				NodeType.ARRAY_TYPED => ReadTypedArray(ctx),
				NodeType.ARRAY_TYPE_BYTE_LENGTH => ReadTypedArray(ctx, true),
				NodeType.ARRAY_TYPE_AUXILIARY_BUFFER => ReadAuxTypedArray(ctx),
				NodeType.OBJECT => ReadObject(ctx),
				NodeType.INT32_AS_BYTE => ReadByteAsInt(ref ctx.Buffer.Bytes1),
				_ => throw new NotSupportedException($"KV3 node {type} not supported"),
			};
			if (inArray)
			{
				// handled by array readers
				return;
			}
			parent[name] = val;
		}

		private static (NodeType, byte) ReadType(Context ctx)
		{
			byte data = ctx.Types[0];
			ctx.Types = ctx.Types[1..];
			byte flag = 0;
			if (ctx.Version >= 3)
			{
				if ((data & 0x80) != 0)
				{
					data &= 0x3F;
					flag = ctx.Types[0];
					ctx.Types = ctx.Types[1..];
				}
			}
			else if ((data & 0x80) != 0)
			{
				data &= 0x7F;
				flag = ctx.Types[0];
				ctx.Types = ctx.Types[1..];
				if ((flag & 4) != 0)
				{
					// multiline string flag -> clear
					flag ^= 4;
				}
			}
			return ((NodeType)data, flag);
		}

		private static bool ReadBool(ref ArraySegment<byte> s)
		{
			bool v = s[0] == 1;
			s = s[1..];
			return v;
		}
		private static ushort ReadU16(ref ArraySegment<byte> s)
		{
			ushort v = MemoryMarshal.Read<ushort>(s);
			s = s[sizeof(ushort)..];
			return v;
		}
		private static float ReadFloat(ref ArraySegment<byte> s)
		{
			float v = MemoryMarshal.Read<float>(s);
			s = s[sizeof(float)..];
			return v;
		}
		private static long ReadInt64(ref ArraySegment<byte> s)
		{
			long v = MemoryMarshal.Read<long>(s);
			s = s[sizeof(long)..];
			return v;
		}
		private static ulong ReadUInt64(ref ArraySegment<byte> s)
		{
			ulong v = MemoryMarshal.Read<ulong>(s);
			s = s[sizeof(ulong)..];
			return v;
		}
		private static double ReadDouble(ref ArraySegment<byte> s)
		{
			double v = MemoryMarshal.Read<double>(s);
			s = s[sizeof(double)..];
			return v;
		}
		private static int ReadByteAsInt(ref ArraySegment<byte> s)
		{
			int v = s[0];
			s = s[1..];
			return v;
		}

		private static string ReadString(Context ctx)
		{
			int id = ReadInt(ref ctx.Buffer.Bytes4);
			return id == -1 ? string.Empty : ctx.Strings[id];
		}

		private static byte[] ReadBinaryBlob(Context ctx)
		{
			int len;
			if (ctx.Version < 2)
			{
				len = ReadInt(ref ctx.Buffer.Bytes4);
				var data = new byte[len];
				Buffer.BlockCopy(ctx.Buffer.Bytes1.Array, ctx.Buffer.Bytes1.Offset, data, 0, len);
				ctx.Buffer.Bytes1 = ctx.Buffer.Bytes1[len..];
				return data;
			}
			else
			{
				len = MemoryMarshal.Read<int>(ctx.BinaryBlobLengths);
				ctx.BinaryBlobLengths = ctx.BinaryBlobLengths[sizeof(int)..];
				var data = new byte[len];
				Buffer.BlockCopy(ctx.BinaryBlobs.Array, ctx.BinaryBlobs.Offset, data, 0, len);
				ctx.BinaryBlobs = ctx.BinaryBlobs[len..];
				return data;
			}
		}

		private static List<object> ReadArray(Context ctx)
		{
			int len = ReadInt(ref ctx.Buffer.Bytes4);
			var list = new List<object>(len);
			for (int i = 0; i < len; i++)
			{
				var (type, flag) = ReadType(ctx);
				object val = type switch
				{
					NodeType.OBJECT => ReadObject(ctx),
					NodeType.ARRAY => ReadArray(ctx),
					NodeType.ARRAY_TYPED => ReadTypedArray(ctx),
					NodeType.ARRAY_TYPE_BYTE_LENGTH => ReadTypedArray(ctx, true),
					NodeType.ARRAY_TYPE_AUXILIARY_BUFFER => ReadAuxTypedArray(ctx),
					NodeType.STRING => ReadString(ctx),
					NodeType.FLOAT => ReadFloat(ref ctx.Buffer.Bytes4),
					NodeType.INT32 => ReadInt(ref ctx.Buffer.Bytes4),
					NodeType.UINT32 => (uint)ReadInt(ref ctx.Buffer.Bytes4),
					NodeType.INT16 => (short)ReadU16(ref ctx.Buffer.Bytes2),
					NodeType.UINT16 => ReadU16(ref ctx.Buffer.Bytes2),
					NodeType.INT64 => ReadInt64(ref ctx.Buffer.Bytes8),
					NodeType.UINT64 => ReadUInt64(ref ctx.Buffer.Bytes8),
					NodeType.DOUBLE => ReadDouble(ref ctx.Buffer.Bytes8),
					NodeType.BOOLEAN => ReadBool(ref ctx.Buffer.Bytes1),
					NodeType.BOOLEAN_TRUE => true,
					NodeType.BOOLEAN_FALSE => false,
					NodeType.BINARY_BLOB => ReadBinaryBlob(ctx),
					NodeType.INT64_ZERO => (long)0,
					NodeType.INT64_ONE => (long)1,
					NodeType.DOUBLE_ZERO => 0.0,
					NodeType.DOUBLE_ONE => 1.0,
					NodeType.NULL => null,
					NodeType.INT32_AS_BYTE => ReadByteAsInt(ref ctx.Buffer.Bytes1),
					_ => throw new NotSupportedException($"KV3 node in array {type} not supported"),
				};
				list.Add(val);
			}
			return list;
		}

		private static List<object> ReadTypedArray(Context ctx, bool byteCountPrefix = false)
		{
			int len = byteCountPrefix ? ctx.Buffer.Bytes1[0] : ReadInt(ref ctx.Buffer.Bytes4);
			if (byteCountPrefix) ctx.Buffer.Bytes1 = ctx.Buffer.Bytes1[1..];
			var (subType, subFlag) = ReadType(ctx);
			var list = new List<object>(len);
			for (int i = 0; i < len; i++)
			{
				// Dispatch per element type similarly to ReadArray
				switch (subType)
				{
					case NodeType.OBJECT: list.Add(ReadObject(ctx)); break;
					case NodeType.ARRAY: list.Add(ReadArray(ctx)); break;
					case NodeType.ARRAY_TYPED: list.Add(ReadTypedArray(ctx)); break;
					case NodeType.ARRAY_TYPE_BYTE_LENGTH: list.Add(ReadTypedArray(ctx, true)); break;
					case NodeType.ARRAY_TYPE_AUXILIARY_BUFFER: list.Add(ReadAuxTypedArray(ctx)); break;
					case NodeType.STRING: list.Add(ReadString(ctx)); break;
					case NodeType.FLOAT: list.Add(ReadFloat(ref ctx.Buffer.Bytes4)); break;
					case NodeType.INT32: list.Add(ReadInt(ref ctx.Buffer.Bytes4)); break;
					case NodeType.UINT32: list.Add((uint)ReadInt(ref ctx.Buffer.Bytes4)); break;
					case NodeType.INT16: list.Add((short)ReadU16(ref ctx.Buffer.Bytes2)); break;
					case NodeType.UINT16: list.Add(ReadU16(ref ctx.Buffer.Bytes2)); break;
					case NodeType.INT64: list.Add(ReadInt64(ref ctx.Buffer.Bytes8)); break;
					case NodeType.UINT64: list.Add(ReadUInt64(ref ctx.Buffer.Bytes8)); break;
					case NodeType.DOUBLE: list.Add(ReadDouble(ref ctx.Buffer.Bytes8)); break;
					case NodeType.BOOLEAN: list.Add(ReadBool(ref ctx.Buffer.Bytes1)); break;
					case NodeType.BOOLEAN_TRUE: list.Add(true); break;
					case NodeType.BOOLEAN_FALSE: list.Add(false); break;
					case NodeType.BINARY_BLOB: list.Add(ReadBinaryBlob(ctx)); break;
					case NodeType.INT64_ZERO: list.Add((long)0); break;
					case NodeType.INT64_ONE: list.Add((long)1); break;
					case NodeType.DOUBLE_ZERO: list.Add(0.0); break;
					case NodeType.DOUBLE_ONE: list.Add(1.0); break;
					case NodeType.INT32_AS_BYTE: list.Add(ReadByteAsInt(ref ctx.Buffer.Bytes1)); break;
					case NodeType.NULL: list.Add(null); break;
					default: throw new NotSupportedException($"KV3 typed array element {subType} not supported");
				}
			}
			return list;
		}

		private static List<object> ReadAuxTypedArray(Context ctx)
		{
			int len = ctx.Buffer.Bytes1[0];
			ctx.Buffer.Bytes1 = ctx.Buffer.Bytes1[1..];
			var (subType, subFlag) = ReadType(ctx);
			(var oldAux, var oldBuf) = (ctx.AuxBuffer, ctx.Buffer);
			(ctx.AuxBuffer, ctx.Buffer) = (ctx.Buffer, ctx.AuxBuffer);
			var list = new List<object>(len);
			for (int i = 0; i < len; i++)
			{
				switch (subType)
				{
					case NodeType.OBJECT: list.Add(ReadObject(ctx)); break;
					case NodeType.ARRAY: list.Add(ReadArray(ctx)); break;
					case NodeType.ARRAY_TYPED: list.Add(ReadTypedArray(ctx)); break;
					case NodeType.ARRAY_TYPE_BYTE_LENGTH: list.Add(ReadTypedArray(ctx, true)); break;
					case NodeType.ARRAY_TYPE_AUXILIARY_BUFFER: list.Add(ReadAuxTypedArray(ctx)); break;
					case NodeType.STRING: list.Add(ReadString(ctx)); break;
					case NodeType.FLOAT: list.Add(ReadFloat(ref ctx.Buffer.Bytes4)); break;
					case NodeType.INT32: list.Add(ReadInt(ref ctx.Buffer.Bytes4)); break;
					case NodeType.UINT32: list.Add((uint)ReadInt(ref ctx.Buffer.Bytes4)); break;
					case NodeType.INT16: list.Add((short)ReadU16(ref ctx.Buffer.Bytes2)); break;
					case NodeType.UINT16: list.Add(ReadU16(ref ctx.Buffer.Bytes2)); break;
					case NodeType.INT64: list.Add(ReadInt64(ref ctx.Buffer.Bytes8)); break;
					case NodeType.UINT64: list.Add(ReadUInt64(ref ctx.Buffer.Bytes8)); break;
					case NodeType.DOUBLE: list.Add(ReadDouble(ref ctx.Buffer.Bytes8)); break;
					case NodeType.BOOLEAN: list.Add(ReadBool(ref ctx.Buffer.Bytes1)); break;
					case NodeType.BOOLEAN_TRUE: list.Add(true); break;
					case NodeType.BOOLEAN_FALSE: list.Add(false); break;
					case NodeType.BINARY_BLOB: list.Add(ReadBinaryBlob(ctx)); break;
					case NodeType.INT64_ZERO: list.Add((long)0); break;
					case NodeType.INT64_ONE: list.Add((long)1); break;
					case NodeType.DOUBLE_ZERO: list.Add(0.0); break;
					case NodeType.DOUBLE_ONE: list.Add(1.0); break;
					case NodeType.INT32_AS_BYTE: list.Add(ReadByteAsInt(ref ctx.Buffer.Bytes1)); break;
					case NodeType.NULL: list.Add(null); break;
					default: throw new NotSupportedException($"KV3 aux typed element {subType} not supported");
				}
			}
			(ctx.AuxBuffer, ctx.Buffer) = (oldAux, oldBuf);
			return list;
		}
	}
}


