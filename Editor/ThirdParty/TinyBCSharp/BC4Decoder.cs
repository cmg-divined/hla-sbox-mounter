using System.Buffers.Binary;

using System;

namespace TinyBCSharp;

abstract class BC4Decoder(bool grayscale)
    : BlockDecoder(8, BytesPerPixel)
{
    const int BytesPerPixel = 4;

    protected void Write(Span<byte> dst, int stride, Span<byte> alphas, long indices)
    {
        if (grayscale)
        {
            WriteRgba(dst, stride, alphas, indices);
        }
        else
        {
            WriteR(dst, stride, alphas, indices);
        }
    }

    static void WriteRgba(Span<byte> dst, int stride, Span<byte> alphas, long indices)
    {
        for (var y = 0; y < BlockHeight; y++)
        {
            var dstPos = y * stride;
            for (var x = 0; x < BlockWidth; x++)
            {
                var index = dstPos + x * BytesPerPixel;
                var alpha = alphas[(int)(indices & 7)];
                var color = (alpha * 0x010101) | unchecked((int)0xFF000000);
                BinaryPrimitives.WriteInt32LittleEndian(dst[index..], color);
                indices >>= 3;
            }
        }
    }

    static void WriteR(Span<byte> dst, int stride, Span<byte> alphas, long indices)
    {
        for (var y = 0; y < BlockHeight; y++)
        {
            var dstPos = y * stride;
            for (var x = 0; x < BlockWidth; x++)
            {
                var index = dstPos + x * BytesPerPixel;
                var alpha = alphas[(int)(indices & 7)];
                dst[index] = alpha;
                indices >>= 3;
            }
        }
    }
}