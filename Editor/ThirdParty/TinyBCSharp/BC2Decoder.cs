using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

class BC2Decoder()
    : BlockDecoder(16, BytesPerPixel)
{
    const int BytesPerPixel = 4;

    static readonly BC1Decoder ColorDecoder = new(BC1Mode.BC2Or3);

    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        ColorDecoder.DecodeBlock(src[8..], dst, stride);
        DecodeAlpha(src, dst[3..], stride);
    }

    static void DecodeAlpha(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        var alphas = BinaryPrimitives.ReadUInt64LittleEndian(src);
        for (var y = 0; y < BlockHeight; y++)
        {
            var dstPos = y * stride;
            for (var x = 0; x < BlockWidth; x++)
            {
                dst[dstPos + x * BytesPerPixel] = (byte)((alphas & 0x0F) * 0x11);
                alphas >>= 4;
            }
        }
    }
}