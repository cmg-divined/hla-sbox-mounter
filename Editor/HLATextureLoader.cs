using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Sandbox.Mounting;
using SkiaSharp;
using HLA;
using HLA.TextureDecoders;
using TinyBCSharp;
using LZ4;
using System.Collections.Generic;

namespace Sandbox
{
	/// <summary>
	/// Comprehensive VTEX_C loader with support for various texture formats and debugging.
	/// </summary>
	internal sealed class HLATextureLoader : ResourceLoader<HlaMount>
	{
		// Static cache for mip sizes (simplified approach for this implementation)
		private static int[] cachedMipSizes;
		private static bool isActuallyCompressedMips;
		private readonly string _vpkPath;
		private readonly HlaMount _host;

		public HLATextureLoader( HlaMount host, string vpkPath )
		{
			_vpkPath = vpkPath;
			_host = host;
		}

		protected override object Load()
		{
			return LoadImmediate();
		}

		public Texture LoadImmediate()
		{
			try
			{
				// Safety check for null/empty paths
				if (string.IsNullOrEmpty(_vpkPath))
				{
					Log.Warning("[HLA] Null or empty texture VPK path");
					return CreateFallbackTexture(64, 64);
				}

				// Log.Info($"[HLA] Loading texture: {_vpkPath}");

				// Read full resource bytes with safety checks
				var fileBytes = _host?.GetFileBytes(_vpkPath);
				if (fileBytes == null || fileBytes.Length == 0)
				{
					Log.Warning($"[HLA] Empty or null file data for texture: {_vpkPath}");
					return CreateFallbackTexture(64, 64);
				}

				using var ms = new MemoryStream(fileBytes);
				using var br = new BinaryReader(ms);

				// Resource header (size, ver)
				uint fileSize = br.ReadUInt32();
				ushort headerVer = br.ReadUInt16();
				ushort typeVer = br.ReadUInt16();
				uint blockOffset = br.ReadUInt32();
				uint blockCount = br.ReadUInt32();

				// Log.Info($"[HLA] Texture resource header: size={fileSize}, headerVer={headerVer}, typeVer={typeVer}, blocks={blockCount}");

				// Find DATA block offset/size
				// Position to start of block directory (VRF does: position += blockOffset - 8)
				// blockOffset is from file start, but we've already read 8 bytes (blockOffset + blockCount)
				ms.Position += blockOffset - 8;
				uint dataOffset = 0;
				uint dataSize = 0;
			Dictionary<string, object> rediData = null;
				
				for ( int i = 0; i < blockCount; i++ )
				{
					uint blockType = br.ReadUInt32();
					long pos = ms.Position;
					uint rel = br.ReadUInt32();
					uint size = br.ReadUInt32();
					
					// Convert blockType to string for debugging
					char[] fourCCChars = new char[4];
					fourCCChars[0] = (char)(blockType & 0xFF);
					fourCCChars[1] = (char)((blockType >> 8) & 0xFF);
					fourCCChars[2] = (char)((blockType >> 16) & 0xFF);
					fourCCChars[3] = (char)((blockType >> 24) & 0xFF);
					string fourCCString = new string(fourCCChars);
					
					Log.Info($"[HLA] Block {i}: type='{fourCCString}' (0x{blockType:X8}), rel={rel}, size={size}");
					
					if ( blockType == (uint)('D' | ('A'<<8) | ('T'<<16) | ('A'<<24)) )
					{
						dataOffset = (uint)(pos + rel);
						dataSize = size;
						Log.Info($"[HLA] Found DATA block: offset=0x{dataOffset:X}, size={dataSize}");
					}
					else if ( blockType == (uint)('R' | ('E'<<8) | ('D'<<16) | ('I'<<24)) )
					{
						// Found REDI block - parse it for texture conversion flags
						Log.Info($"[HLA] Found REDI block: offset=0x{(pos + rel):X}, size={size}");
						
						long savedPos = ms.Position;
						ms.Position = pos + rel;
						
						try
						{
							// Parse REDI block using our custom parser
							var rediBytes = br.ReadBytes((int)size);
							rediData = REDIParser.ParseREDI(rediBytes);
						}
						catch (Exception ex)
						{
							Log.Warning($"[HLA] Failed to parse REDI block: {ex.Message}");
						}
						
						ms.Position = savedPos;
					}
					// Each block entry is 12 bytes: type(4) + offset(4) + size(4)
				}

				if (dataOffset == 0)
				{
					Log.Warning($"[HLA] No DATA block found in texture {_vpkPath}");
					return CreateFallbackTexture(64, 64);
				}

				// Read VTEX header
				ms.Position = dataOffset;
				var vtex = ReadVTexHeader(br);
				
				// Log.Info($"[HLA] VTex header: version={vtex.Version}, format={vtex.Format} ({(byte)vtex.Format}), size={vtex.Width}x{vtex.Height}x{vtex.Depth}, mips={vtex.NumMipLevels}, flags=0x{(ushort)vtex.Flags:X}");

				// Handle embedded formats first
				if (vtex.Format == VTexFormat.PNG_RGBA8888 || vtex.Format == VTexFormat.PNG_DXT5)
				{
					ms.Position = dataOffset;
					var png = ReadEmbeddedImage( ms, 0x474E5089, 0x0A1A0A0D );
					if ( png != null )
					{
						Log.Info($"[HLA] Successfully extracted embedded PNG from {_vpkPath}");
						return CreateTextureFromImageBytes( png );
					}
				}
				
				if (vtex.Format == VTexFormat.JPEG_RGBA8888 || vtex.Format == VTexFormat.JPEG_DXT5)
				{
					ms.Position = dataOffset;
					var jpg = ReadEmbeddedJpeg( ms );
					if ( jpg != null )
					{
						Log.Info($"[HLA] Successfully extracted embedded JPEG from {_vpkPath}");
						return CreateTextureFromImageBytes( jpg );
					}
				}

				// Calculate data start position (after VTEX header and extra data)
				var textureDataOffset = CalculateTextureDataOffset(br, dataOffset, vtex);
				ms.Position = textureDataOffset;

				// Decode texture based on format
				var texture = DecodeTexture(br, vtex, textureDataOffset, rediData);
				if (texture != null)
				{
					// Log.Info($"[HLA] Successfully decoded texture {_vpkPath} with format {vtex.Format}");
					return texture;
				}

				// Fallback: create placeholder
				Log.Warning($"[HLA] Unsupported texture format {vtex.Format} for {_vpkPath}, creating placeholder");
				return CreateFallbackTexture(vtex.Width, vtex.Height);
			}
			catch ( Exception e )
			{
				Log.Warning( $"[HLA] Texture loading failed for {_vpkPath}: {e.Message}" );
				Log.Warning( $"[HLA] Exception details: {e}" );
				return CreateFallbackTexture(64, 64);
			}
		}

		private struct VTexHeader
		{
			public ushort Version;
			public VTexFlags Flags;
			public float[] Reflectivity; // 4 floats
			public ushort Width;
			public ushort Height;
			public ushort Depth;
			public VTexFormat Format;
			public byte NumMipLevels;
			public uint Picmip0Res;
			public uint ExtraDataOffset;
			public uint ExtraDataCount;
		}

		private static VTexHeader ReadVTexHeader(BinaryReader br)
		{
			var header = new VTexHeader();
			header.Version = br.ReadUInt16();
			header.Flags = (VTexFlags)br.ReadUInt16();
			header.Reflectivity = new float[4];
			for (int i = 0; i < 4; i++)
			{
				header.Reflectivity[i] = br.ReadSingle();
			}
			header.Width = br.ReadUInt16();
			header.Height = br.ReadUInt16();
			header.Depth = br.ReadUInt16();
			header.Format = (VTexFormat)br.ReadByte();
			header.NumMipLevels = br.ReadByte();
			header.Picmip0Res = br.ReadUInt32();
			header.ExtraDataOffset = br.ReadUInt32();
			header.ExtraDataCount = br.ReadUInt32();
			return header;
		}

		private static long CalculateTextureDataOffset(BinaryReader br, uint dataOffset, VTexHeader vtex)
		{
			// Start after the VTEX header (which we just read)
			long offset = br.BaseStream.Position;
			
			// Skip extra data if present
			if (vtex.ExtraDataCount > 0)
			{
				// Position to extra data directory (like VRF does)
				br.BaseStream.Position += vtex.ExtraDataOffset - 8; // 8 is 2 uint32s we just read for offset and count
				
				// Track the maximum offset to find where texture data starts
				long maxDataEnd = br.BaseStream.Position;
				
				// Read each extra data entry
				for (uint i = 0; i < vtex.ExtraDataCount; i++)
				{
					var type = (VTexExtraData)br.ReadUInt32();
					var entryOffset = br.ReadUInt32() - 8; // Relative offset minus the 8 bytes we read
					var size = br.ReadUInt32();
					
					Log.Info($"[HLA] Extra data entry {i}: type={type}, size={size}");
					
					// For COMPRESSED_MIP_SIZE, we need to read the mip sizes to calculate offsets
					if (type == VTexExtraData.COMPRESSED_MIP_SIZE)
					{
						var savedPos = br.BaseStream.Position;
						br.BaseStream.Position += entryOffset;
						
						var int1 = br.ReadUInt32();
						var mipsOffset = br.ReadUInt32(); 
						var mipCount = br.ReadUInt32();
						
						// VRF logic: IsActuallyCompressedMips = int1 == 1
						isActuallyCompressedMips = int1 == 1;
						
						Log.Info($"[HLA] COMPRESSED_MIP_SIZE: int1={int1}, mipsOffset={mipsOffset}, mipCount={mipCount}");
						Log.Info($"[HLA] IsActuallyCompressedMips: {isActuallyCompressedMips}");
						
						// Read the mip size array
						br.BaseStream.Position += mipsOffset - 8; // Adjust for the 8 bytes we read
						
						var mipSizes = new int[mipCount];
						for (int m = 0; m < mipCount; m++)
						{
							mipSizes[m] = br.ReadInt32();
						}
						
						Log.Info($"[HLA] Mip sizes: [{string.Join(", ", mipSizes)}]");
						
						// Store mip sizes for DecodeTexture to use
						cachedMipSizes = mipSizes;
						
						// The texture data starts after all extra data
						var mipSizeTableEnd = br.BaseStream.Position;
						maxDataEnd = Math.Max(maxDataEnd, mipSizeTableEnd);
						
						br.BaseStream.Position = savedPos;
					}
					else
					{
						// Calculate end of this data block
						long entryEnd = br.BaseStream.Position + entryOffset + size;
						maxDataEnd = Math.Max(maxDataEnd, entryEnd);
					}
				}
				
				// Texture data starts after all extra data
				offset = maxDataEnd;
			}
			
			return offset;
		}

		private static Texture DecodeTexture(BinaryReader br, VTexHeader vtex, long dataOffset, Dictionary<string, object> rediData)
		{
			br.BaseStream.Position = dataOffset;
			
			// Skip to mip level 0 (largest) using compressed mip sizes
			// Mips are stored from smallest (mip N-1) to largest (mip 0)
			// cachedMipSizes[i] contains the compressed size of mip level i
			
			var mip0DataOffset = dataOffset;
			
			// Use cached mip sizes if available
			try
			{
				if (cachedMipSizes != null && vtex.NumMipLevels > 1 && cachedMipSizes.Length == vtex.NumMipLevels)
				{
					// Skip all smaller mips (from N-1 down to 1) to reach mip 0
					long skipBytes = 0;
					for (int mip = vtex.NumMipLevels - 1; mip > 0; mip--)
					{
						skipBytes += cachedMipSizes[mip];
					}
					
					mip0DataOffset = dataOffset + skipBytes;
					Log.Info($"[HLA] Skipping {skipBytes} bytes of smaller mips to reach mip 0 (compressed sizes: mips {vtex.NumMipLevels-1} down to 1)");
				}
			}
			catch (Exception ex)
			{
				Log.Warning($"[HLA] Failed to calculate mip offset: {ex.Message}");
			}
			
			br.BaseStream.Position = mip0DataOffset;
			
			// Handle LZ4 decompression if needed
			byte[] data;
			int uncompressedSize = CalculateBufferSize(vtex.Width, vtex.Height, vtex.Format);
			
			if (isActuallyCompressedMips && cachedMipSizes != null && cachedMipSizes.Length > 0)
			{
				// VRF logic: if compressedSize >= uncompressedSize, data is stored uncompressed
				int compressedSize = cachedMipSizes[0];
				long remainingBytes = br.BaseStream.Length - br.BaseStream.Position;
				int bytesToRead = Math.Min(compressedSize, (int)remainingBytes);
				
				if (compressedSize >= uncompressedSize)
				{
					// Data is stored uncompressed despite the compressed mips flag
					data = br.ReadBytes(Math.Min(uncompressedSize, (int)remainingBytes));
					Log.Info($"[HLA] Reading uncompressed data (compressedSize >= uncompressedSize): size={uncompressedSize}, read={data.Length} bytes");
				}
				else
				{
					// Data is actually LZ4-compressed
					var compressedData = br.ReadBytes(bytesToRead);
					Log.Info($"[HLA] Reading LZ4-compressed data: compressedSize={compressedSize}, read={compressedData.Length} bytes");
					
					// LZ4 decompress to get raw texture data
					try
					{
						data = LZ4Codec.Decode(compressedData, 0, compressedData.Length, uncompressedSize);
						Log.Info($"[HLA] LZ4 decompressed: {compressedData.Length} -> {data.Length} bytes (expected {uncompressedSize})");
					}
					catch (Exception ex)
					{
						Log.Error($"[HLA] LZ4 decompression failed: {ex.Message}");
						return CreateFallbackTexture(vtex.Width, vtex.Height);
					}
				}
			}
			else
			{
				// Read uncompressed data directly
				long remainingBytes = br.BaseStream.Length - br.BaseStream.Position;
				int bytesToRead = Math.Min(uncompressedSize, (int)remainingBytes);
				
				data = br.ReadBytes(bytesToRead);
				Log.Info($"[HLA] Reading uncompressed data: size={uncompressedSize}, read={data.Length} bytes");
			}
			
			Log.Info($"[HLA] Final texture data: format={vtex.Format}, size={vtex.Width}x{vtex.Height}, dataLength={data.Length} bytes");
			
			// Debug: log first 32 bytes of texture data
			if (data.Length >= 32)
			{
				var firstBytes = string.Join(" ", data.Take(32).Select(b => $"{b:X2}"));
				// Log.Info($"[HLA] First 32 bytes: {firstBytes}");
			}
			
			// Create SKBitmap for decoding
			var bitmap = new SKBitmap(vtex.Width, vtex.Height, SKColorType.Bgra8888, SKAlphaType.Unpremul);
			
			try
			{
				// Decode based on format
				var decoder = CreateDecoder(vtex.Format, vtex.Width, vtex.Height);
				if (decoder != null)
				{
					decoder.Decode(bitmap, data);
					
					// Apply texture conversions based on REDI SpecialDependencies
					var codec = RetrieveCodecFromResourceEditInfo(rediData, vtex.Format);
					if (codec != TextureCodec.None)
					{
						Log.Info($"[HLA] Applying texture conversions: {codec}");
						Common.ApplyTextureConversions(bitmap, codec);
					}
					
					// Convert to s&box texture
					var result = CreateTextureFromBitmap(bitmap);
					return result;
				}
			}
			catch (Exception ex)
			{
				Log.Warning($"[HLA] Failed to decode texture with format {vtex.Format}: {ex.Message}");
			}
			finally
			{
				bitmap?.Dispose();
			}
			
			return null;
		}

		private static ITextureDecoder CreateDecoder(VTexFormat format, int width, int height)
		{
			return format switch
			{
				// Basic color formats
				VTexFormat.RGBA8888 => new DecodeRGBA8888(),
				VTexFormat.BGRA8888 => new DecodeBGRA8888(),
				VTexFormat.I8 => new DecodeI8(),
				VTexFormat.IA88 => new DecodeIA88(),
				
				// Block compression formats
				VTexFormat.DXT1 => new DecodeBCn(width, height, TinyBCSharp.BlockFormat.BC1NoAlpha),
				VTexFormat.DXT5 => new DecodeBCn(width, height, TinyBCSharp.BlockFormat.BC3),
				VTexFormat.ATI1N => new DecodeBCn(width, height, TinyBCSharp.BlockFormat.BC4U),
				VTexFormat.ATI2N => new DecodeBCn(width, height, TinyBCSharp.BlockFormat.BC5U),
				VTexFormat.BC6H => new DecodeBCn(width, height, TinyBCSharp.BlockFormat.BC6HUf32),
				VTexFormat.BC7 => new DecodeBCn(width, height, TinyBCSharp.BlockFormat.BC7),
				
				// ETC formats
				VTexFormat.ETC2 => new DecodeETC2(width, height),
				VTexFormat.ETC2_EAC => new DecodeETC2EAC(width, height),
				
				// Single channel formats
				VTexFormat.R16 => new DecodeR16(),
				VTexFormat.R16F => new DecodeR16F(),
				VTexFormat.R32F => new DecodeR32F(),
				
				// Dual channel formats
				VTexFormat.RG1616 => new DecodeRG1616(),
				VTexFormat.RG1616F => new DecodeRG1616F(),
				VTexFormat.RG3232F => new DecodeRG3232F(),
				
				// Triple channel formats
				VTexFormat.RGB323232F => new DecodeRGB323232F(),
				
				// High precision formats
				VTexFormat.RGBA16161616 => new DecodeRGBA16161616(),
				VTexFormat.RGBA16161616F => new DecodeRGBA16161616F(),
				VTexFormat.RGBA32323232F => new DecodeRGBA32323232F(),
				
				_ => null
			};
		}

		private static int CalculateBufferSize(int width, int height, VTexFormat format)
		{
			// Block compression formats
			if (IsBlockCompressionFormat(format))
			{
				var blocksX = (width + 3) / 4;
				var blocksY = (height + 3) / 4;
				
				return format switch
				{
					VTexFormat.DXT1 or VTexFormat.ETC2 or VTexFormat.ATI1N => Math.Max(8, blocksX * blocksY * 8),
					VTexFormat.DXT5 or VTexFormat.ETC2_EAC or VTexFormat.BC6H or VTexFormat.BC7 => Math.Max(16, blocksX * blocksY * 16),
					VTexFormat.ATI2N => Math.Max(16, blocksX * blocksY * 16),
					_ => blocksX * blocksY * 16 // Default for unknown block formats
				};
			}
			
			// Uncompressed formats
			int bytesPerPixel = format switch
			{
				VTexFormat.RGBA8888 or VTexFormat.BGRA8888 => 4,
				VTexFormat.I8 => 1,
				VTexFormat.IA88 => 2,
				VTexFormat.R16 or VTexFormat.R16F => 2,
				VTexFormat.RG1616 or VTexFormat.RG1616F => 4,
				VTexFormat.RGBA16161616 or VTexFormat.RGBA16161616F => 8,
				VTexFormat.R32F => 4,
				VTexFormat.RG3232F => 8,
				VTexFormat.RGB323232F => 12,
				VTexFormat.RGBA32323232F => 16,
				_ => 4 // Default to RGBA8888
			};
			
			return width * height * bytesPerPixel;
		}

		private static bool IsBlockCompressionFormat(VTexFormat format)
		{
			return format switch
			{
				VTexFormat.DXT1 or VTexFormat.DXT5 or
				VTexFormat.ATI1N or VTexFormat.ATI2N or
				VTexFormat.BC6H or VTexFormat.BC7 or
				VTexFormat.ETC2 or VTexFormat.ETC2_EAC => true,
				_ => false
			};
		}

		private static Texture CreateTextureFromBitmap(SKBitmap bitmap)
		{
			var pixels = bitmap.GetPixelSpan();
			var data = new byte[pixels.Length];
			pixels.CopyTo(data);
			
			return Texture.Create(bitmap.Width, bitmap.Height)
				.WithFormat(ImageFormat.BGRA8888)
				.WithData(data)
				.Finish();
		}

		private static Texture CreateFallbackTexture(int width, int height)
		{
			width = Math.Max(1, width);
			height = Math.Max(1, height);
			
			var rgba = new byte[width * height * 4];
			for (int i = 0; i < rgba.Length; i += 4)
			{
				rgba[i + 0] = 128;  // R
				rgba[i + 1] = 128;  // G  
				rgba[i + 2] = 128;  // B
				rgba[i + 3] = 255;  // A
			}
			
			return Texture.Create(width, height)
				.WithFormat(ImageFormat.RGBA8888)
				.WithData(rgba)
				.Finish();
		}

		private static byte[] ReadEmbeddedImage( Stream s, int sigA, int sigB )
		{
			var br = new BinaryReader( s );
			long start = s.Position;
			while ( s.Position + 8 < s.Length )
			{
				long p = s.Position;
				int a = br.ReadInt32();
				int b = br.ReadInt32();
				if ( a == sigA && b == sigB )
				{
					s.Position = p;
					// Read to end-of-file; caller will decode
					var bytes = new byte[s.Length - p];
					s.Read( bytes, 0, bytes.Length );
					return bytes;
				}
			}
			s.Position = start;
			return null;
		}

		private static byte[] ReadEmbeddedJpeg( Stream s )
		{
			var br = new BinaryReader( s );
			long start = s.Position;
			while ( s.Position + 2 < s.Length )
			{
				byte b0 = br.ReadByte();
				byte b1 = br.ReadByte();
				if ( b0 == 0xFF && b1 == 0xD8 ) // SOI
				{
					s.Position -= 2;
					var bytes = new byte[s.Length - s.Position];
					s.Read( bytes, 0, bytes.Length );
					return bytes;
				}
			}
			s.Position = start;
			return null;
		}

		private static Texture CreateTextureFromImageBytes( byte[] data )
		{
			using var bitmap = SKBitmap.Decode( data );
			if ( bitmap == null || bitmap.Width <= 0 || bitmap.Height <= 0 )
			{
				return CreateFallbackTexture(64, 64);
			}

			var info = new SKImageInfo( bitmap.Width, bitmap.Height, SKColorType.Rgba8888, SKAlphaType.Unpremul );
			using var rgba = new SKBitmap( info );
			if ( !bitmap.CopyTo( rgba, SKColorType.Rgba8888 ) )
			{
				return CreateFallbackTexture(64, 64);
			}

			var ptr = rgba.GetPixels();
			int len = info.BytesSize;
			var pixels = new byte[len];
			Marshal.Copy( ptr, pixels, 0, len );

			return Texture.Create( info.Width, info.Height )
				.WithFormat( ImageFormat.RGBA8888 )
				.WithData( pixels )
				.Finish();
		}
		
		/// <summary>
		/// Retrieve texture codec flags from the resource edit info (REDI block)
		/// </summary>
		private static TextureCodec RetrieveCodecFromResourceEditInfo(Dictionary<string, object> rediData, VTexFormat format)
		{
			var codec = TextureCodec.None;
			
			if (rediData == null || !rediData.TryGetValue("SpecialDependencies", out var specialDepsObj))
			{
				return codec;
			}

			if (!(specialDepsObj is Dictionary<string, object> specialDeps))
			{
				return codec;
			}

			// Parse through SpecialDependencies entries
			foreach (var kvp in specialDeps)
			{
				if (kvp.Value is Dictionary<string, object> dependency)
				{
					if (dependency.TryGetValue("CompilerIdentifier", out var compilerIdObj) &&
						dependency.TryGetValue("String", out var stringObj))
					{
						var compilerIdentifier = compilerIdObj.ToString();
						var processorString = stringObj.ToString();
						
						Log.Info($"[HLA] SpecialDependency - CompilerIdentifier: '{compilerIdentifier}', String: '{processorString}'");
						
						// Check that this is a texture compilation dependency and use the String field
						if (compilerIdentifier == "CompileTexture" && !string.IsNullOrEmpty(processorString))
						{
							Log.Info($"[HLA] Processing texture conversion: '{processorString}'");
							
							codec |= processorString switch
							{
								// Image processor algorithms
								"Texture Compiler Version Image YCoCg Conversion" => TextureCodec.YCoCg,
								"Texture Compiler Version Image NormalizeNormals" => TextureCodec.NormalizeNormals,

								// Mipmap processor algorithms  
								"Texture Compiler Version Mip HemiOctIsoRoughness_RG_B" => TextureCodec.HemiOctRB,
								"Texture Compiler Version Mip HemiOctAnisoRoughness" => TextureCodec.HemiOctRB, // do we lose one of the roughness components? (anisotropic is xy)
								_ => TextureCodec.None,
							};
						}
					}
				}
			}

			// Additional format-specific logic from VRF
			if (format == VTexFormat.DXT5 && codec.HasFlag(TextureCodec.NormalizeNormals))
			{
				codec |= TextureCodec.Dxt5nm;
			}
			else if (format == VTexFormat.BC7 && codec.HasFlag(TextureCodec.HemiOctRB)
									  && codec.HasFlag(TextureCodec.NormalizeNormals))
			{
				// VRF removes NormalizeNormals for BC7+HemiOct - HemiOct handles reconstruction
				codec &= ~TextureCodec.NormalizeNormals;
				// Log.Info($"[HLA] BC7+HemiOct detected: removing NormalizeNormals flag");
			}

				// Log.Info($"[HLA] Detected texture codec flags: {codec}");
			return codec;
		}
	}
}


