// C# 11 version (no primary constructor)
using System;

namespace TinyBCSharp
{
    abstract class BC5Decoder : BlockDecoder
    {
        private readonly BC4Decoder _decoder;
        private readonly bool _reconstructZ;
        private const int BytesPerPixel = 4;

        protected BC5Decoder(BC4Decoder decoder, bool reconstructZ)
            : base(16, BytesPerPixel)
        {
            _decoder = decoder;
            _reconstructZ = reconstructZ;
        }

        public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
        {
            _decoder.DecodeBlock(src, dst, stride);
            _decoder.DecodeBlock(src.Slice(8), dst.Slice(1), stride);
            WriteAlphas(dst.Slice(3), stride);

            if (_reconstructZ)
            {
                ReconstructZ.Reconstruct(dst, stride, BytesPerPixel);
            }
        }

        private static void WriteAlphas(Span<byte> dst, int stride)
        {
            for (var y = 0; y < BlockHeight; y++)
            {
                var dstPos = y * stride;
                for (var x = 0; x < BlockWidth; x++)
                {
                    dst[dstPos + x * BytesPerPixel] = 0xFF;
                }
            }
        }
    }
}
