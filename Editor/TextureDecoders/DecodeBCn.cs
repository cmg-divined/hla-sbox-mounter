using System;
using SkiaSharp;
using TinyBCSharp;

namespace HLA.TextureDecoders
{

    // BCn decoder using TinyBCSharp library
    internal readonly struct DecodeBCn : ITextureDecoder
    {
        readonly int w;
        readonly int h;
        readonly TinyBCSharp.BlockFormat format;

        public DecodeBCn(int w, int h, TinyBCSharp.BlockFormat format)
        {
            this.w = w;
            this.h = h;
            this.format = format;
        }

        public void Decode(SKBitmap bitmap, Span<byte> input)
        {
            using var pixmap = bitmap.PeekPixels();
            var data = pixmap.GetPixelSpan<byte>();

            // Use TinyBCSharp for decoding
            var decoder = TinyBCSharp.BlockDecoder.Create(format);
            
            // Calculate blocks
            var blocksX = (w + 3) / 4;
            var blocksY = (h + 3) / 4;
            var blockSize = format switch
            {
                TinyBCSharp.BlockFormat.BC1 
                    or TinyBCSharp.BlockFormat.BC1NoAlpha 
                    or TinyBCSharp.BlockFormat.BC4U 
                    or TinyBCSharp.BlockFormat.BC4S => 8,
                _ => 16
            };
            
            // Decode each block
            for (int by = 0; by < blocksY; by++)
            {
                for (int bx = 0; bx < blocksX; bx++)
                {
                    var blockIndex = (by * blocksX + bx) * blockSize;
                    if (blockIndex + blockSize > input.Length) break;

                    var block = input.Slice(blockIndex, blockSize);
                    
                    // Calculate destination offset
                    var dstX = bx * 4;
                    var dstY = by * 4;
                    var dstOffset = (dstY * bitmap.Width + dstX) * 4;
                    
                    if (dstOffset < data.Length)
                    {
                        var dstSpan = data.Slice(dstOffset);
                        decoder.DecodeBlock(block, dstSpan, bitmap.Width * 4);
                    }
                }
            }

            // BCn decoders typically produce RGBA, we need BGRA for s&box
            if (bitmap.ColorType == SKColorType.Bgra8888)
            {
                Common.SwapRB(data);
            }
        }

    }
}