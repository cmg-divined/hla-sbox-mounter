using System;

namespace TinyBCSharp;

class BC3Decoder()
    : BlockDecoder(16, BytesPerPixel)
{
    const int BytesPerPixel = 4;

    static readonly BC1Decoder ColorDecoder = new(BC1Mode.BC2Or3);
    static readonly BC4UDecoder AlphaDecoder = new(false);

    public override void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride)
    {
        ColorDecoder.DecodeBlock(src[8..], dst, stride);
        AlphaDecoder.DecodeBlock(src, dst[3..], stride);
    }
}