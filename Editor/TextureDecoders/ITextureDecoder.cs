using System;
using SkiaSharp;

namespace HLA.TextureDecoders
{
    internal interface ITextureDecoder
    {
        public abstract void Decode(SKBitmap bitmap, Span<byte> input);
    }
}
