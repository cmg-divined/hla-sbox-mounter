using System;

namespace Sandbox
{
    public enum VTexFormat : byte
    {
        UNKNOWN = 0,
        DXT1 = 1,
        DXT5 = 2,
        I8 = 3,
        RGBA8888 = 4,
        R16 = 5,
        RG1616 = 6,
        RGBA16161616 = 7,
        R16F = 8,
        RG1616F = 9,
        RGBA16161616F = 10,
        R32F = 11,
        RG3232F = 12,
        RGB323232F = 13,
        RGBA32323232F = 14,
        JPEG_RGBA8888 = 15,
        PNG_RGBA8888 = 16,
        JPEG_DXT5 = 17,
        PNG_DXT5 = 18,
        BC6H = 19,
        BC7 = 20,
        ATI2N = 21,
        IA88 = 22,
        ETC2 = 23,
        ETC2_EAC = 24,
        R11_EAC = 25,
        RG11_EAC = 26,
        ATI1N = 27,
        BGRA8888 = 28,
        WEBP_RGBA8888 = 29,
        WEBP_DXT5 = 30,
    }

    [Flags]
    public enum VTexFlags : ushort
    {
        SUGGEST_CLAMPS = 0x00000001,
        SUGGEST_CLAMPT = 0x00000002,
        SUGGEST_CLAMPZ = 0x00000004,
        NO_LOD = 0x00000008,
        CUBE_TEXTURE = 0x00000010,
        VOLUME_TEXTURE = 0x00000020,
        TEXTURE_ARRAY = 0x00000040,
    }

    public enum VTexExtraData : uint
    {
        UNKNOWN = 0,
        FALLBACK_BITS = 1,
        SHEET = 2,
        FILL_TO_POWER_OF_TWO = 3,
        COMPRESSED_MIP_SIZE = 4,
        CUBEMAP_RADIANCE_SH = 5,
        METADATA = 6,
    }
}
