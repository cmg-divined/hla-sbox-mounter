using System;

namespace TinyBCSharp;

public abstract class BlockDecoder(int bytesPerBlock, int bytesPerPixel)
{
    internal const int BlockWidth = 4;
    internal const int BlockHeight = 4;

    public static BlockDecoder Create(BlockFormat format)
    {
        return format switch
        {
            BlockFormat.BC1 => new BC1Decoder(BC1Mode.Normal),
            BlockFormat.BC1NoAlpha => new BC1Decoder(BC1Mode.Opaque),
            BlockFormat.BC2 => new BC2Decoder(),
            BlockFormat.BC3 => new BC3Decoder(),
            BlockFormat.BC4U => new BC4UDecoder(true),
            BlockFormat.BC4S => new BC4SDecoder(true),
            BlockFormat.BC5U => new BC5UDecoder(false),
            BlockFormat.BC5UReconstructZ => new BC5UDecoder(true),
            BlockFormat.BC5S => new BC5SDecoder(false),
            BlockFormat.BC5SReconstructZ => new BC5SDecoder(true),
            BlockFormat.BC6HUf16 => new BC6HDecoder(false, false),
            BlockFormat.BC6HSf16 => new BC6HDecoder(true, false),
            BlockFormat.BC6HUf32 => new BC6HDecoder(false, true),
            BlockFormat.BC6HSf32 => new BC6HDecoder(true, true),
            BlockFormat.BC7 => new BC7Decoder(),
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, null)
        };
    }

    public abstract void DecodeBlock(ReadOnlySpan<byte> src, Span<byte> dst, int stride);

    public byte[] Decode(int width, int height, ReadOnlySpan<byte> src)
    {
        var size = width * height * bytesPerPixel;
        var dst = new byte[size];
        Decode(src, width, height, dst);
        return dst;
    }

    public void Decode(ReadOnlySpan<byte> src, int width, int height, Span<byte> dst)
    {
        Decode(src, width, height, dst, width, height);
    }

    public void Decode(
        ReadOnlySpan<byte> src, int srcWidth, int srcHeight,
        Span<byte> dst, int dstWidth, int dstHeight)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(srcWidth);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(srcHeight);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dstWidth);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(dstHeight);

        ArgumentOutOfRangeException.ThrowIfLessThan(src.Length, SourceSize(bytesPerBlock, srcWidth, srcHeight));
        ArgumentOutOfRangeException.ThrowIfLessThan(dst.Length, TargetSize(bytesPerPixel, dstWidth, dstHeight));

        var rowStride = dstWidth * bytesPerPixel;
        for (int y = 0, srcPos = 0; y < dstHeight; y += BlockHeight)
        {
            var dstPos = y * rowStride;
            for (var x = 0; x < srcWidth; x += BlockWidth, srcPos += bytesPerBlock)
            {
                if (x >= dstWidth)
                {
                    continue;
                }

                var dstOffset = dstPos + x * bytesPerPixel;
                if (y + BlockHeight <= dstHeight && x + BlockWidth <= dstWidth)
                {
                    DecodeBlock(src[srcPos..], dst[dstOffset..], rowStride);
                }
                else
                {
                    PartialBlock(src[srcPos..], dstWidth, dstHeight, dst[dstOffset..], x, y, rowStride);
                }
            }
        }
    }

    void PartialBlock(
        ReadOnlySpan<byte> src, int width, int height,
        Span<byte> dst, int x, int y, int rowStride)
    {
        var blockStride = bytesPerPixel * BlockWidth;
        var block = (stackalloc byte[BlockHeight * blockStride]);
        DecodeBlock(src, block, blockStride);

        var partialHeight = Math.Min(height - y, BlockHeight);
        var partialStride = Math.Min(width - x, BlockWidth) * bytesPerPixel;
        for (var yy = 0; yy < partialHeight; yy++)
        {
            block
                .Slice(yy * blockStride, partialStride)
                .CopyTo(dst[(yy * rowStride)..]);
        }
    }

    public static int SourceSize(int width, int height, BlockFormat format)
    {
        var bytesPerBlock = format switch
        {
            BlockFormat.BC1
                or BlockFormat.BC1NoAlpha
                or BlockFormat.BC4U
                or BlockFormat.BC4S => 8,
            _ => 16
        };

        return SourceSize(width, height, bytesPerBlock);
    }

    static int SourceSize(int width, int height, int bytesPerBlock)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(width);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(height);

        var widthInBlocks = (width + 3) / 4;
        var heightInBlocks = (height + 3) / 4;
        return widthInBlocks * heightInBlocks * bytesPerBlock;
    }

    public static int TargetSize(int width, int height, BlockFormat format)
    {
        var bytesPerPixel = format switch
        {
            BlockFormat.BC6HUf16 or BlockFormat.BC6HSf16 => 8,
            BlockFormat.BC6HUf32 or BlockFormat.BC6HSf32 => 16,
            _ => 4
        };
        return TargetSize(width, height, bytesPerPixel);
    }

    static int TargetSize(int width, int height, int bytesPerPixel)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(width);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(height);

        return width * height * bytesPerPixel;
    }
}