using System;
using System.Collections.Generic;
using System.IO;
using Sandbox;
using Sandbox.Mounting;
using HLA;

namespace Sandbox
{
	internal sealed class HLAMaterialLoader : ResourceLoader<HlaMount>
	{
		private readonly string _vpkPath;
		private readonly HlaMount _host;
		private Dictionary<string, float> _materialFloatParams;
		private Dictionary<string, Vector4> _materialVectorParams;
		private string _materialShaderName;

		public HLAMaterialLoader(HlaMount host, string vpkPath)
		{
			_vpkPath = vpkPath;
			_host = host;
		}

		public Material LoadImmediate()
		{
			return Load() as Material;
		}

		protected override object Load()
		{
			try
			{
				// Log.Info($"[HLA] Loading material: {_vpkPath}");

				using var fileStream = _host.GetFileStream(_vpkPath);
				using var reader = new BinaryReader(fileStream);

				// Read VResource header (similar to texture loading)
				var size = reader.ReadUInt32();
				var headerVersion = reader.ReadUInt16();
				var version = reader.ReadUInt16();
				var blockOffset = reader.ReadUInt32();
				var blockCount = reader.ReadUInt32();

				// Log.Info($"[HLA] Material resource header: size={size}, headerVer={headerVersion}, typeVer={version}, blocks={blockCount}");

				// Find DATA block (contains the material data) - following VRF's approach
				reader.BaseStream.Position = blockOffset; // Position to start of block directory

				Dictionary<string, string> compiledTextures = null;

				for (int i = 0; i < blockCount; i++)
				{
					var blockType = reader.ReadUInt32();
					var blockPos = reader.BaseStream.Position;
					var relativeOffset = reader.ReadUInt32(); // This is relative offset like in texture loader
					var blockDataSize = reader.ReadUInt32();
					// Skip CRC32
					reader.ReadUInt32();

					// Calculate absolute offset
					var blockDataOffset = blockPos + relativeOffset;

					// Convert blockType to readable string (little-endian)
					var blockTypeBytes = BitConverter.GetBytes(blockType);
					var blockTypeStr = System.Text.Encoding.ASCII.GetString(blockTypeBytes);
					Log.Info($"[HLA] Block {i}: type='{blockTypeStr}' (0x{blockType:X8}), rel={relativeOffset}, abs_offset={blockDataOffset}, size={blockDataSize}");

					if (blockTypeStr == "DATA")
					{
						Log.Info($"[HLA] Found material DATA block: offset=0x{blockDataOffset:X}, size={blockDataSize}");

						var currentPos = reader.BaseStream.Position;
						reader.BaseStream.Position = blockDataOffset;

						// Read the material data and parse it
						var materialBytes = reader.ReadBytes((int)blockDataSize);
						Log.Info($"[HLA] Read {materialBytes.Length} bytes of material data");
						Log.Info($"[HLA] First 32 bytes: {string.Join(" ", materialBytes.Take(32).Select(b => b.ToString("X2")))}");
						
						// Try to interpret as text
						var asText = System.Text.Encoding.UTF8.GetString(materialBytes);
						var firstChars = asText.Length > 200 ? asText.Substring(0, 200) : asText;
						Log.Info($"[HLA] First 200 chars as text: {firstChars}");
						
						var (textures, floatParams, vectorParams, shaderName) = ParseMaterialData(materialBytes);
						compiledTextures = textures;
						_materialFloatParams = floatParams;
						_materialVectorParams = vectorParams;
						_materialShaderName = shaderName;

						reader.BaseStream.Position = currentPos;
						break;
					}
				}

				if (compiledTextures != null && compiledTextures.Count > 0)
				{
					return CreateMaterialFromTextures(compiledTextures, _materialFloatParams, _materialVectorParams, _materialShaderName, _vpkPath);
				}
				else
				{
					Log.Warning($"[HLA] No compiled textures found in material: {_vpkPath}");
					return CreateFallbackMaterial();
				}
			}
			catch (Exception ex)
			{
				Log.Error($"[HLA] Failed to load material {_vpkPath}: {ex.Message}");
				return CreateFallbackMaterial();
			}
		}

		private static (Dictionary<string, string> textures, Dictionary<string, float> floatParams, Dictionary<string, Vector4> vectorParams, string shaderName) ParseMaterialData(byte[] data)
		{
			var compiledTextures = new Dictionary<string, string>();
			var floatParams = new Dictionary<string, float>();
			var vectorParams = new Dictionary<string, Vector4>();
			string shaderName = null;

			try
			{
				Log.Info($"[HLA] Attempting to parse {data.Length} bytes as binary KV3");
				
				// Try to parse as binary KV3 (like VRF does for materials)
				var materialData = BinaryKV3Lite.Parse(data);
				
				Log.Info($"[HLA] Successfully parsed binary KV3, found {materialData.Count} root keys");
				foreach (var key in materialData.Keys.Take(10))
				{
					Log.Info($"[HLA] Root key: {key}");
				}

				// Extract texture parameters
				if (materialData.TryGetValue("m_textureParams", out var textureParamsObj) &&
					textureParamsObj is List<object> textureParams)
				{
					Log.Info($"[HLA] Found m_textureParams with {textureParams.Count} entries");
					
					foreach (var paramObj in textureParams)
					{
						if (paramObj is Dictionary<string, object> param)
						{
							if (param.TryGetValue("m_name", out var nameObj) &&
								param.TryGetValue("m_pValue", out var valueObj))
							{
								var paramName = nameObj?.ToString();
								var paramValue = valueObj?.ToString();
								
								if (!string.IsNullOrEmpty(paramName) && !string.IsNullOrEmpty(paramValue))
								{
									compiledTextures[paramName] = paramValue;
									Log.Info($"[HLA] Texture param: {paramName} = {paramValue}");
								}
							}
						}
					}
				}

				// Extract int parameters first (might contain transform info)
				if (materialData.TryGetValue("m_intParams", out var intParamsObj) &&
					intParamsObj is List<object> intParamsList)
				{
					foreach (var param in intParamsList)
					{
						if (param is Dictionary<string, object> intParam)
						{
							if (intParam.TryGetValue("m_name", out var nameObj) && nameObj is string name &&
								intParam.TryGetValue("m_nValue", out var valueObj))
							{
								if (valueObj is int intValue)
								{
									// Convert int params to float for shader compatibility
									floatParams[name] = (float)intValue;
									Log.Info($"[HLA] Int param: {name} = {intValue} (converted to float)");
								}
								else if (valueObj is long longValue)
								{
									floatParams[name] = (float)longValue;
									Log.Info($"[HLA] Int param: {name} = {longValue} (converted from long to float)");
								}
							}
						}
					}
				}

				// Extract float parameters
				if (materialData.TryGetValue("m_floatParams", out var floatParamsObj) &&
					floatParamsObj is List<object> floatParamsList)
				{
					foreach (var param in floatParamsList)
					{
						if (param is Dictionary<string, object> floatParam)
						{
							if (floatParam.TryGetValue("m_name", out var nameObj) && nameObj is string name &&
								floatParam.TryGetValue("m_flValue", out var valueObj))
							{
								if (valueObj is float floatValue)
								{
									floatParams[name] = floatValue;
									Log.Info($"[HLA] Float param: {name} = {floatValue}");
								}
								else if (valueObj is double doubleValue)
								{
									floatParams[name] = (float)doubleValue;
									Log.Info($"[HLA] Float param: {name} = {doubleValue} (converted from double)");
								}
							}
						}
					}
				}

				// Extract vector parameters
				if (materialData.TryGetValue("m_vectorParams", out var vectorParamsObj) &&
					vectorParamsObj is List<object> vectorParamsList)
				{
					foreach (var param in vectorParamsList)
					{
						if (param is Dictionary<string, object> vectorParam)
						{
							if (vectorParam.TryGetValue("m_name", out var nameObj) && nameObj is string name &&
								vectorParam.TryGetValue("m_value", out var valueObj) && valueObj is Dictionary<string, object> valueDict)
							{
								// Extract vector components
								float x = 0, y = 0, z = 0, w = 0;
								if (valueDict.TryGetValue("x", out var xObj)) x = Convert.ToSingle(xObj);
								if (valueDict.TryGetValue("y", out var yObj)) y = Convert.ToSingle(yObj);
								if (valueDict.TryGetValue("z", out var zObj)) z = Convert.ToSingle(zObj);
								if (valueDict.TryGetValue("w", out var wObj)) w = Convert.ToSingle(wObj);
								
								vectorParams[name] = new Vector4(x, y, z, w);
								Log.Info($"[HLA] Vector param: {name} = ({x}, {y}, {z}, {w})");
							}
						}
					}
				}

				// Extract shader name
				if (materialData.TryGetValue("m_shaderName", out var shaderNameObj))
				{
					shaderName = shaderNameObj?.ToString();
					Log.Info($"[HLA] Shader: {shaderName}");
				}

				Log.Info($"[HLA] Extracted {compiledTextures.Count} compiled textures, {floatParams.Count} float params, {vectorParams.Count} vector params from KV3");
				
				// Debug: log all float params we found
				if (floatParams.Count > 0)
				{
					Log.Info($"[HLA] Float parameters found:");
					foreach (var fp in floatParams)
					{
						Log.Info($"[HLA]   {fp.Key} = {fp.Value}");
					}
				}
				
				// Debug: log all vector params we found
				if (vectorParams.Count > 0)
				{
					Log.Info($"[HLA] Vector parameters found:");
					foreach (var vp in vectorParams)
					{
						Log.Info($"[HLA]   {vp.Key} = {vp.Value}");
					}
				}
				else
				{
					Log.Info($"[HLA] No vector parameters found - checking if they're stored under different keys");
					// Check other possible keys where UV transforms might be stored
					foreach (var key in materialData.Keys)
					{
						Log.Info($"[HLA] Available material key: {key} (type: {materialData[key]?.GetType()?.Name})");
					}
				}
			}
			catch (Exception ex)
			{
				Log.Error($"[HLA] Failed to parse as KV3: {ex.Message}");
				
				// If KV3 parsing fails, try to see what the data actually contains
				var asText = System.Text.Encoding.UTF8.GetString(data.Take(200).ToArray());
				Log.Info($"[HLA] First 200 bytes as text: {asText}");
				
				// Log the first few bytes as hex for debugging
				var hexBytes = string.Join(" ", data.Take(32).Select(b => b.ToString("X2")));
				Log.Info($"[HLA] First 32 bytes as hex: {hexBytes}");
			}

			return (compiledTextures, floatParams, vectorParams, shaderName);
		}

		private static void ExtractCompiledTextures(Dictionary<string, object> layerData, Dictionary<string, string> compiledTextures)
		{
			// Look for various texture parameter names that might contain compiled textures
			var textureParams = new[]
			{
				"g_tColor", "g_tAlbedo", "g_tBaseColor",
				"g_tNormal", "g_tBumpmap",
				"g_tRoughness", "g_tMetallic", "g_tSpecular",
				"g_tAmbientOcclusion", "g_tAO",
				"g_tEmissive", "g_tSelfIllum",
				"g_tHeight", "g_tDisplacement"
			};

			foreach (var param in textureParams)
			{
				if (layerData.TryGetValue(param, out var textureObj) && textureObj != null)
				{
					var texturePath = textureObj.ToString();
					if (!string.IsNullOrEmpty(texturePath) && texturePath.EndsWith(".vtex"))
					{
						compiledTextures[param] = texturePath;
						Log.Info($"[HLA] Found texture parameter: {param} = {texturePath}");
					}
				}
			}

			// Also recursively search nested dictionaries
			foreach (var kvp in layerData)
			{
				if (kvp.Value is Dictionary<string, object> nestedDict)
				{
					ExtractCompiledTextures(nestedDict, compiledTextures);
				}
			}
		}

		private Material CreateMaterialFromTextures(Dictionary<string, string> compiledTextures, Dictionary<string, float> floatParams, Dictionary<string, Vector4> vectorParams, string shaderName, string materialPath)
		{
			try
			{
				// Create material with proper shader
				var materialName = System.IO.Path.GetFileNameWithoutExtension(materialPath);
				var material = Material.Create(materialName, "shaders/complex.shader");
				
				Log.Info($"[HLA] Creating material '{materialName}' with {compiledTextures.Count} compiled textures:");
				foreach (var kvp in compiledTextures)
				{
					Log.Info($"[HLA]   {kvp.Key} -> {kvp.Value}");
				}

				// Only handle the core textures to avoid crashes
				if (compiledTextures.TryGetValue("g_tColor", out var colorTexture))
				{
					var texture = LoadCompiledTexture(colorTexture);
					if (texture != null)
					{
						material.Set("g_tColor", texture);
						Log.Info($"[HLA] Set color texture: {colorTexture}");
					}
					else
					{
						Log.Warning($"[HLA] Failed to load color texture: {colorTexture}");
					}
				}
				else
				{
					Log.Warning($"[HLA] No g_tColor found in compiled textures");
				}

				if (compiledTextures.TryGetValue("g_tNormal", out var normalTexture))
				{
					var texture = LoadCompiledTexture(normalTexture);
					if (texture != null)
					{
						material.Set("g_tNormal", texture);
						Log.Info($"[HLA] Set normal texture: {normalTexture}");
					}
					else
					{
						Log.Warning($"[HLA] Failed to load normal texture: {normalTexture}");
						// Set fallback neutral normal map (flat normal pointing up)
						material.Set("g_tNormal", CreateFlatNormalTexture());
					}
				}
				else
				{
					Log.Warning($"[HLA] No g_tNormal found in compiled textures, using default flat normal");
					material.Set("g_tNormal", CreateFlatNormalTexture());
				}

				if (compiledTextures.TryGetValue("g_tAmbientOcclusion", out var aoTexture))
				{
					var texture = LoadCompiledTexture(aoTexture);
					if (texture != null)
					{
						material.Set("g_tAmbientOcclusion", texture);
						Log.Info($"[HLA] Set AO texture: {aoTexture}");
					}
					else
					{
						Log.Warning($"[HLA] Failed to load AO texture: {aoTexture}");
						// Set fallback white AO map (no occlusion)
						material.Set("g_tAmbientOcclusion", Texture.White);
					}
				}
				else
				{
					Log.Warning($"[HLA] No g_tAmbientOcclusion found in compiled textures, using white");
					material.Set("g_tAmbientOcclusion", Texture.White);
				}

				// Apply shader flags and parameters from the s&box material format
				material.Set("F_METALNESS_TEXTURE", 1f);
				material.Set("F_SPECULAR", 1f);
				
				// Apply float parameters from material
				foreach (var floatParam in floatParams)
				{
					material.Set(floatParam.Key, floatParam.Value);
					Log.Info($"[HLA] Applied float param: {floatParam.Key} = {floatParam.Value}");
				}
				
				// Apply vector parameters from material
				foreach (var vectorParam in vectorParams)
				{
					material.Set(vectorParam.Key, vectorParam.Value);
					Log.Info($"[HLA] Applied vector param: {vectorParam.Key} = {vectorParam.Value}");
				}
				
				// Set default parameters only if not already set by material
				if (!floatParams.ContainsKey("g_bFogEnabled"))
					material.Set("g_bFogEnabled", 1f);
				if (!floatParams.ContainsKey("g_nScaleTexCoordUByModelScaleAxis"))
					material.Set("g_nScaleTexCoordUByModelScaleAxis", 0f);
				if (!floatParams.ContainsKey("g_nScaleTexCoordVByModelScaleAxis"))
					material.Set("g_nScaleTexCoordVByModelScaleAxis", 0f);
				if (!floatParams.ContainsKey("g_flAmbientOcclusionDirectDiffuse"))
					material.Set("g_flAmbientOcclusionDirectDiffuse", 0f);
				if (!floatParams.ContainsKey("g_flAmbientOcclusionDirectSpecular"))
					material.Set("g_flAmbientOcclusionDirectSpecular", 0f);
				if (!floatParams.ContainsKey("g_flModelTintAmount"))
					material.Set("g_flModelTintAmount", 1f);
				if (!floatParams.ContainsKey("g_flFadeExponent"))
					material.Set("g_flFadeExponent", 1f);
				if (!floatParams.ContainsKey("g_flRoughnessScaleFactor"))
					material.Set("g_flRoughnessScaleFactor", 1f);
				
				if (!vectorParams.ContainsKey("g_vColorTint"))
					material.Set("g_vColorTint", new Vector4(1f, 1f, 1f, 0f));
				if (!vectorParams.ContainsKey("g_vTexCoordOffset"))
					material.Set("g_vTexCoordOffset", new Vector4(0f, 0f, 0f, 0f));
				if (!vectorParams.ContainsKey("g_vTexCoordScale"))
					material.Set("g_vTexCoordScale", new Vector4(1f, 1f, 0f, 0f));
				
				// Note: Removed experimental texture addressing parameters
				// UV coordinates > 1.0 should wrap naturally in s&box
				if (!vectorParams.ContainsKey("g_vTexCoordScrollSpeed"))
					material.Set("g_vTexCoordScrollSpeed", new Vector4(0f, 0f, 0f, 0f));

				Log.Info($"[HLA] Created material '{materialName}' with {compiledTextures.Count} textures and shader flags");
				return material;
			}
			catch (Exception ex)
			{
				Log.Error($"[HLA] Error creating material: {ex.Message}");
				return CreateFallbackMaterial();
			}
		}

		private Texture LoadCompiledTexture(string texturePath)
		{
			try
			{
				// Safety check for null/empty paths
				if (string.IsNullOrEmpty(texturePath))
				{
					Log.Warning("[HLA] Null or empty texture path");
					return CreateFallbackTexture();
				}

				// Add .vtex_c extension if not present
				if (!texturePath.EndsWith(".vtex_c"))
				{
					texturePath += "_c";
				}

				// Log.Info($"[HLA] Loading compiled texture: {texturePath}");
				
				// Check if file exists in our VPK mount before trying to load
				try
				{
					var fileData = _host?.GetFileBytes(texturePath);
					if (fileData == null || fileData.Length == 0)
					{
						Log.Warning($"[HLA] Empty or null file data for: {texturePath}");
						return CreateFallbackTexture();
					}

					// Log.Info($"[HLA] Found texture file {texturePath}, size: {fileData.Length} bytes");
					
					// Create texture loader directly like GTASA does
					var textureLoader = new HLATextureLoader(_host, texturePath);
					
					try
					{
						var texture = textureLoader.LoadImmediate();
						
						if (texture != null)
						{
							// Log.Info($"[HLA] Successfully loaded texture: {texturePath}");
							return texture;
						}
						else
						{
							Log.Warning($"[HLA] Texture loader returned null for: {texturePath}");
							return CreateFallbackTexture();
						}
					}
					catch (Exception loadEx)
					{
						Log.Warning($"[HLA] Texture loading failed for {texturePath}: {loadEx.Message}");
						return CreateFallbackTexture();
					}
				}
				catch (FileNotFoundException)
				{
					Log.Warning($"[HLA] Texture file not found in VPKs: {texturePath}");
					return CreateFallbackTexture();
				}
				catch (Exception fileEx)
				{
					Log.Warning($"[HLA] File access error for {texturePath}: {fileEx.Message}");
					return CreateFallbackTexture();
				}
			}
			catch (Exception ex)
			{
				Log.Warning($"[HLA] Failed to load compiled texture {texturePath}: {ex.Message}");
				return CreateFallbackTexture();
			}
		}

		private Texture CreateFallbackTexture()
		{
			try
			{
				// Create a simple gray fallback texture
				var grayData = new byte[128 * 128 * 4];
				for (int i = 0; i < grayData.Length; i += 4)
				{
					grayData[i] = 128;     // R
					grayData[i + 1] = 128; // G
					grayData[i + 2] = 128; // B
					grayData[i + 3] = 255; // A
				}
				
				return Texture.Create(128, 128)
					.WithFormat(ImageFormat.RGBA8888)
					.WithData(grayData)
					.Finish();
			}
			catch
			{
				return Texture.White;
			}
		}

		private static Material CreateFallbackMaterial()
		{
			var material = Material.Create("hla_fallback", "shaders/complex");
			material.Set("g_tColor", Texture.White);
			return material;
		}

		private static Texture CreateFlatNormalTexture()
		{
			// Create a 1x1 flat normal map texture (RGB = 0.5, 0.5, 1.0 normalized to 128, 128, 255)
			var normalData = new byte[] { 128, 128, 255, 255 }; // RGBA format, flat normal pointing up
			return Texture.Create(1, 1, ImageFormat.RGBA8888)
				.WithData(normalData)
				.Finish();
		}
	}
}