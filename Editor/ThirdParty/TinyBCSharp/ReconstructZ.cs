using System;

namespace TinyBCSharp;

static class ReconstructZ
{
    static readonly byte[] Normal = new byte[256 * 256];

    static ReconstructZ()
    {
        for (var y = 0; y < 256; y++)
        {
            for (var x = 0; x < 256; x++)
            {
                var r = x * (1.0f / 127.5f) - 1.0f;
                var g = y * (1.0f / 127.5f) - 1.0f;
                var b = (float)Math.Sqrt(1.0f - Math.Min(1.0f, Math.Max(r * r + g * g, 0.0f)));
                Normal[(y << 8) + x] = (byte)(b * 127.5f + 128.0f);
            }
        }
    }

    internal static void Reconstruct(Span<byte> dst, int lineStride, int pixelStride)
    {
        for (var y = 0; y < BlockDecoder.BlockHeight; y++)
        {
            var dstPos = y * lineStride;
            for (var x = 0; x < BlockDecoder.BlockWidth; x++)
            {
                var i = dstPos + x * pixelStride;
                int r = dst[i + 0];
                int g = dst[i + 1];
                dst[i + 2] = Normal[(g << 8) + r];
            }
        }
    }
}