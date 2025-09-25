using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Sandbox
{
	internal static class MeshOptimizerVertexDecoder
	{
		private const byte VertexHeader = 0xa0;
		private const int VertexBlockSizeBytes = 8192;
		private const int VertexBlockMaxSize = 256;
		private const int ByteGroupSize = 16;
		private const int ByteGroupDecodeLimit = 24;
		private const int TailMaxSize = 32;

		private static int GetVertexBlockSize( int vertexSize )
		{
			var result = VertexBlockSizeBytes / vertexSize;
			result &= ~(ByteGroupSize - 1);
			return result < VertexBlockMaxSize ? result : VertexBlockMaxSize;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static byte Unzigzag8( byte v )
		{
			return (byte)(-(v & 1) ^ (v >> 1));
		}

		private static Span<byte> DecodeBytesGroup( Span<byte> data, Span<byte> destination, int bitslog2 )
		{
			int dataVar;
			byte b;
			byte enc;

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			byte Next( byte bits, byte encv )
			{
				enc = b;
				enc >>= 8 - bits;
				b <<= bits;
				if ( enc == (1 << bits) - 1 )
				{
					dataVar += 1;
					return encv;
				}
				return enc;
			}

			switch ( bitslog2 )
			{
				case 0:
					for ( var k = 0; k < ByteGroupSize; k++ ) destination[k] = 0;
					return data;
				case 1:
					dataVar = 4;
					b = data[0];
					destination[0] = Next( 2, data[dataVar] );
					destination[1] = Next( 2, data[dataVar] );
					destination[2] = Next( 2, data[dataVar] );
					destination[3] = Next( 2, data[dataVar] );
					b = data[1];
					destination[4] = Next( 2, data[dataVar] );
					destination[5] = Next( 2, data[dataVar] );
					destination[6] = Next( 2, data[dataVar] );
					destination[7] = Next( 2, data[dataVar] );
					b = data[2];
					destination[8] = Next( 2, data[dataVar] );
					destination[9] = Next( 2, data[dataVar] );
					destination[10] = Next( 2, data[dataVar] );
					destination[11] = Next( 2, data[dataVar] );
					b = data[3];
					destination[12] = Next( 2, data[dataVar] );
					destination[13] = Next( 2, data[dataVar] );
					destination[14] = Next( 2, data[dataVar] );
					destination[15] = Next( 2, data[dataVar] );
					return data[dataVar..];
				case 2:
					dataVar = 8;
					b = data[0];
					destination[0] = Next( 4, data[dataVar] );
					destination[1] = Next( 4, data[dataVar] );
					b = data[1];
					destination[2] = Next( 4, data[dataVar] );
					destination[3] = Next( 4, data[dataVar] );
					b = data[2];
					destination[4] = Next( 4, data[dataVar] );
					destination[5] = Next( 4, data[dataVar] );
					b = data[3];
					destination[6] = Next( 4, data[dataVar] );
					destination[7] = Next( 4, data[dataVar] );
					b = data[4];
					destination[8] = Next( 4, data[dataVar] );
					destination[9] = Next( 4, data[dataVar] );
					b = data[5];
					destination[10] = Next( 4, data[dataVar] );
					destination[11] = Next( 4, data[dataVar] );
					b = data[6];
					destination[12] = Next( 4, data[dataVar] );
					destination[13] = Next( 4, data[dataVar] );
					b = data[7];
					destination[14] = Next( 4, data[dataVar] );
					destination[15] = Next( 4, data[dataVar] );
					return data[dataVar..];
				case 3:
					data[..ByteGroupSize].CopyTo( destination );
					return data[ByteGroupSize..];
				default:
					throw new ArgumentException( "Unexpected bit length" );
			}
		}

		private static Span<byte> DecodeBytes( Span<byte> data, Span<byte> destination )
		{
			if ( destination.Length % ByteGroupSize != 0 ) throw new ArgumentException( "Expected data length to be a multiple of ByteGroupSize." );
			var headerSize = ((destination.Length / ByteGroupSize) + 3) / 4;
			var header = data[..];
			data = data[headerSize..];
			for ( var i = 0; i < destination.Length; i += ByteGroupSize )
			{
				if ( data.Length < ByteGroupDecodeLimit ) throw new InvalidOperationException( "Cannot decode" );
				var headerOffset = i / ByteGroupSize;
				var bitslog2 = (header[headerOffset / 4] >> (headerOffset % 4 * 2)) & 3;
				data = DecodeBytesGroup( data, destination[i..], bitslog2 );
			}
			return data;
		}

		private static Span<byte> DecodeVertexBlock( Span<byte> data, Span<byte> vertexData, int vertexCount, int vertexSize, Span<byte> lastVertex )
		{
			if ( vertexCount <= 0 || vertexCount > VertexBlockMaxSize ) throw new ArgumentException( "Expected vertexCount to be between 0 and VertexMaxBlockSize" );
			var vertexCountAligned = (vertexCount + ByteGroupSize - 1) & ~(ByteGroupSize - 1);
			var bufferPool = ArrayPool<byte>.Shared.Rent( VertexBlockMaxSize );
			var buffer = bufferPool.AsSpan( 0, VertexBlockMaxSize );
			var transposedPool = ArrayPool<byte>.Shared.Rent( VertexBlockSizeBytes );
			var transposed = transposedPool.AsSpan( 0, VertexBlockSizeBytes );
			try
			{
				for ( var k = 0; k < vertexSize; ++k )
				{
					data = DecodeBytes( data, buffer[..vertexCountAligned] );
					var vertexOffset = k;
					var p = lastVertex[k];
					for ( var i = 0; i < vertexCount; ++i )
					{
						var v = (byte)(Unzigzag8( buffer[i] ) + p);
						transposed[vertexOffset] = v;
						p = v;
						vertexOffset += vertexSize;
					}
				}
				transposed[..(vertexCount * vertexSize)].CopyTo( vertexData );
				transposed.Slice( vertexSize * (vertexCount - 1), vertexSize ).CopyTo( lastVertex );
			}
			finally
			{
				ArrayPool<byte>.Shared.Return( bufferPool );
				ArrayPool<byte>.Shared.Return( transposedPool );
			}
			return data;
		}

		public static byte[] DecodeVertexBuffer( int vertexCount, int vertexSize, Span<byte> buffer )
		{
			if ( vertexSize <= 0 || vertexSize > 256 ) throw new ArgumentException( "Vertex size is expected to be between 1 and 256" );
			if ( vertexSize % 4 != 0 ) throw new ArgumentException( "Vertex size is expected to be a multiple of 4." );
			if ( buffer.Length < 1 + vertexSize ) throw new ArgumentException( "Vertex buffer is too short." );
			if ( (buffer[0] & 0xF0) != VertexHeader ) throw new ArgumentException( $"Invalid vertex buffer header, expected {VertexHeader} but got {buffer[0]}." );
			var version = buffer[0] & 0x0F;
			if ( version > 0 ) throw new ArgumentException( $"Incorrect vertex buffer encoding version, got {version}." );
			buffer = buffer[1..];
			var resultArray = new byte[vertexCount * vertexSize];
			var lastVertexBuffer = ArrayPool<byte>.Shared.Rent( vertexSize );
			var lastVertex = lastVertexBuffer.AsSpan( 0, vertexSize );
			try
			{
				buffer.Slice( buffer.Length - vertexSize, vertexSize ).CopyTo( lastVertex );
				var vertexBlockSize = GetVertexBlockSize( vertexSize );
				var vertexOffset = 0;
				var result = resultArray.AsSpan();
				while ( vertexOffset < vertexCount )
				{
					var blockSize = vertexOffset + vertexBlockSize < vertexCount ? vertexBlockSize : vertexCount - vertexOffset;
					var vertexData = result[(vertexOffset * vertexSize)..];
					buffer = DecodeVertexBlock( buffer, vertexData, blockSize, vertexSize, lastVertex );
					vertexOffset += blockSize;
				}
			}
			finally
			{
				ArrayPool<byte>.Shared.Return( lastVertexBuffer );
			}
			var tailSize = vertexSize < TailMaxSize ? TailMaxSize : vertexSize;
			if ( buffer.Length != tailSize ) throw new ArgumentException( "Tail size incorrect" );
			return resultArray;
		}
	}

	internal static class MeshOptimizerIndexDecoder
	{
		private const byte IndexHeader = 0xe0;

		private static void PushEdgeFifo( Span<ValueTuple<uint, uint>> fifo, ref int offset, uint a, uint b )
		{
			fifo[offset] = (a, b);
			offset = (offset + 1) & 15;
		}

		private static void PushVertexFifo( Span<uint> fifo, ref int offset, uint v, bool cond = true )
		{
			fifo[offset] = v;
			offset = (offset + (cond ? 1 : 0)) & 15;
		}

		private static uint DecodeVByte( Span<byte> data, ref int position )
		{
			var lead = (uint)data[position++];
			if ( lead < 128 ) return lead;
			var result = lead & 127;
			var shift = 7;
			for ( var i = 0; i < 4; i++ )
			{
				var group = (uint)data[position++];
				result |= (group & 127) << shift;
				shift += 7;
				if ( group < 128 ) break;
			}
			return result;
		}

		private static uint DecodeIndex( Span<byte> data, uint last, ref int position )
		{
			var v = DecodeVByte( data, ref position );
			var d = (uint)((v >> 1) ^ -(v & 1));
			return last + d;
		}

		private static void WriteTriangle( Span<byte> destination, int offset, int indexSize, uint a, uint b, uint c )
		{
			offset *= indexSize;
			if ( indexSize == 2 )
			{
				BinaryPrimitives.WriteUInt16LittleEndian( destination[(offset + 0)..], (ushort)a );
				BinaryPrimitives.WriteUInt16LittleEndian( destination[(offset + 2)..], (ushort)b );
				BinaryPrimitives.WriteUInt16LittleEndian( destination[(offset + 4)..], (ushort)c );
			}
			else
			{
				BinaryPrimitives.WriteUInt32LittleEndian( destination[(offset + 0)..], a );
				BinaryPrimitives.WriteUInt32LittleEndian( destination[(offset + 4)..], b );
				BinaryPrimitives.WriteUInt32LittleEndian( destination[(offset + 8)..], c );
			}
		}

		public static byte[] DecodeIndexBuffer( int indexCount, int indexSize, Span<byte> buffer )
		{
			if ( indexCount % 3 != 0 ) throw new ArgumentException( "Expected indexCount to be a multiple of 3." );
			if ( indexSize != 2 && indexSize != 4 ) throw new ArgumentException( "Expected indexSize to be either 2 or 4" );
			var dataOffset = 1 + (indexCount / 3);
			if ( buffer.Length < dataOffset + 16 ) throw new ArgumentException( "Index buffer is too short." );
			if ( (buffer[0] & 0xF0) != IndexHeader ) throw new ArgumentException( $"Invalid index buffer header, expected {IndexHeader} but got {buffer[0]}." );
			var version = buffer[0] & 0x0F;
			if ( version > 1 ) throw new ArgumentException( $"Incorrect index buffer encoding version, got {version}." );
			Span<uint> vertexFifo = stackalloc uint[16];
			Span<ValueTuple<uint, uint>> edgeFifo = stackalloc ValueTuple<uint, uint>[16];
			var edgeFifoOffset = 0;
			var vertexFifoOffset = 0;
			var next = 0u;
			var last = 0u;
			var fecmax = version >= 1 ? 13 : 15;
			var bufferIndex = 1;
			var data = buffer[dataOffset..^16];
			var codeauxTable = buffer[^16..];
			var destinationArray = new byte[indexCount * indexSize];
			var destination = destinationArray.AsSpan();
			var position = 0;
			for ( var i = 0; i < indexCount; i += 3 )
			{
				var codetri = buffer[bufferIndex++];
				if ( codetri < 0xf0 )
				{
					var fe = codetri >> 4;
					var (a, b) = edgeFifo[(edgeFifoOffset - 1 - fe) & 15];
					var fec = codetri & 15;
					if ( fec < fecmax )
					{
						var c = fec == 0 ? next : vertexFifo[(vertexFifoOffset - 1 - fec) & 15];
						var fec0 = fec == 0;
						next += fec0 ? 1u : 0u;
						WriteTriangle( destination, i, indexSize, a, b, c );
						PushVertexFifo( vertexFifo, ref vertexFifoOffset, c, fec0 );
						PushEdgeFifo( edgeFifo, ref edgeFifoOffset, c, b );
						PushEdgeFifo( edgeFifo, ref edgeFifoOffset, a, c );
					}
					else
					{
						var c = last = (fec != 15) ? last + (uint)(fec - (fec ^ 3)) : DecodeIndex( data, last, ref position );
						WriteTriangle( destination, i, indexSize, a, b, c );
						PushVertexFifo( vertexFifo, ref vertexFifoOffset, c );
						PushEdgeFifo( edgeFifo, ref edgeFifoOffset, c, b );
						PushEdgeFifo( edgeFifo, ref edgeFifoOffset, a, c );
					}
				}
				else if ( codetri < 0xfe )
				{
					var codeaux = codeauxTable[codetri & 15];
					var feb = codeaux >> 4;
					var fec = codeaux & 15;
					var a = next++;
					var b = (feb == 0) ? next : vertexFifo[(vertexFifoOffset - feb) & 15];
					var feb0 = feb == 0 ? 1u : 0u;
					next += feb0;
					var c = (fec == 0) ? next : vertexFifo[(vertexFifoOffset - fec) & 15];
					var fec0 = fec == 0 ? 1u : 0u;
					next += fec0;
					WriteTriangle( destination, i, indexSize, a, b, c );
					PushVertexFifo( vertexFifo, ref vertexFifoOffset, a );
					PushVertexFifo( vertexFifo, ref vertexFifoOffset, b, feb0 == 1u );
					PushVertexFifo( vertexFifo, ref vertexFifoOffset, c, fec0 == 1u );
					PushEdgeFifo( edgeFifo, ref edgeFifoOffset, b, a );
					PushEdgeFifo( edgeFifo, ref edgeFifoOffset, c, b );
					PushEdgeFifo( edgeFifo, ref edgeFifoOffset, a, c );
				}
				else
				{
					var codeaux = (uint)data[position++];
					var fea = codetri == 0xfe ? 0 : 15;
					var feb = codeaux >> 4;
					var fec = codeaux & 15;
					if ( codeaux == 0 ) next = 0;
					var a = (fea == 0) ? next++ : 0;
					var b = (feb == 0) ? next++ : vertexFifo[(vertexFifoOffset - (int)feb) & 15];
					var c = (fec == 0) ? next++ : vertexFifo[(vertexFifoOffset - (int)fec) & 15];
					if ( fea == 15 ) last = a = DecodeIndex( data, last, ref position );
					if ( feb == 15 ) last = b = DecodeIndex( data, last, ref position );
					if ( fec == 15 ) last = c = DecodeIndex( data, last, ref position );
					WriteTriangle( destination, i, indexSize, a, b, c );
					PushVertexFifo( vertexFifo, ref vertexFifoOffset, a );
					PushVertexFifo( vertexFifo, ref vertexFifoOffset, b, (feb == 0) || (feb == 15) );
					PushVertexFifo( vertexFifo, ref vertexFifoOffset, c, (fec == 0) || (fec == 15) );
					PushEdgeFifo( edgeFifo, ref edgeFifoOffset, b, a );
					PushEdgeFifo( edgeFifo, ref edgeFifoOffset, c, b );
					PushEdgeFifo( edgeFifo, ref edgeFifoOffset, a, c );
				}
			}
			if ( position != data.Length ) throw new System.IO.InvalidDataException( "index decode: trailing data" );
			return destinationArray;
		}
	}
}




