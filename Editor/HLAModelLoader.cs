using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Runtime.InteropServices;
using Sandbox;
using Sandbox.Mounting;

namespace Sandbox
{
	/// <summary>
	/// Skinned vertex structure for HLA models with blend indices and weights
	/// </summary>
	[StructLayout(LayoutKind.Sequential)]
	internal struct HLASkinnedVertex
	{
		[VertexLayout.Position]
		public Vector3 position;

		[VertexLayout.Normal] 
		public Vector3 normal;

		[VertexLayout.Tangent]
		public Vector3 tangent;

		[VertexLayout.TexCoord]
		public Vector2 texcoord;

		[VertexLayout.BlendIndices]
		public Color32 blendIndices;

		[VertexLayout.BlendWeight]
		public Color32 blendWeights;

		public static readonly VertexAttribute[] Layout = new VertexAttribute[6]
		{
			new VertexAttribute(VertexAttributeType.Position, VertexAttributeFormat.Float32),
			new VertexAttribute(VertexAttributeType.Normal, VertexAttributeFormat.Float32),
			new VertexAttribute(VertexAttributeType.Tangent, VertexAttributeFormat.Float32),
			new VertexAttribute(VertexAttributeType.TexCoord, VertexAttributeFormat.Float32, 2),
			new VertexAttribute(VertexAttributeType.BlendIndices, VertexAttributeFormat.UInt8, 4),
			new VertexAttribute(VertexAttributeType.BlendWeights, VertexAttributeFormat.UInt8, 4)
		};

		public HLASkinnedVertex(Vector3 position, Vector3 normal, Vector3 tangent, Vector2 texcoord, Color32 blendIndices, Color32 blendWeights)
		{
			this.position = position;
			this.normal = normal;
			this.tangent = tangent;
			this.texcoord = texcoord;
			this.blendIndices = blendIndices;
			this.blendWeights = blendWeights;
		}
	}

	/// <summary>
	/// Minimal VMdl/VMesh parser: reads VBIB block (uncompressed) to build a mesh with positions and indices.
	/// </summary>
	internal sealed class HLAModelLoader : ResourceLoader<HlaMount>
	{
		private readonly string _vpkPath;
		private static readonly bool EnableNonAnimLogs = false;
		private static Dictionary<string,object> cachedDataChannelArray = null;
		public HLAModelLoader( HlaMount host, string vpkPath ) { _vpkPath = vpkPath; }

		protected override object Load()
		{
			var startTime = System.Diagnostics.Stopwatch.StartNew();
			try
			{
				// Safety check for null/empty paths
				if (string.IsNullOrEmpty(_vpkPath))
				{
					Log.Warning("[HLA] Null or empty model VPK path");
					return CreatePlaceholderTriangle();
				}

				if (EnableNonAnimLogs) Log.Info($"[HLA] Load model {_vpkPath}");
				
				// Safety check for file data
				var bytes = base.Host?.GetFileBytes(_vpkPath);
				if (bytes == null || bytes.Length == 0)
				{
					Log.Warning($"[HLA] Empty or null file data for model: {_vpkPath}");
					return CreatePlaceholderTriangle();
				}

				// Safety check: validate file size
				if (bytes.Length < 32)
				{
					Log.Warning($"[HLA] File too small ({bytes.Length} bytes): {_vpkPath}");
					return CreatePlaceholderTriangle();
				}

				// Safety timeout check
				if (startTime.ElapsedMilliseconds > 30000) // 30 second timeout
				{
					Log.Warning($"[HLA] Model loading timeout after {startTime.ElapsedMilliseconds}ms: {_vpkPath}");
					return CreatePlaceholderTriangle();
				}

				// Storage for physics data to be processed after builder is created
				(Dictionary<string,object> physKV, List<object> partsList)? physicsPartsData = null;
				(Dictionary<string,object> physKV, List<object> jointsList)? physicsJointsData = null;
				
				using var ms = new MemoryStream( bytes );
				using var br = new BinaryReader( ms );

				// Resource header - with safety checks
				uint fileSize, blockOffset, blockCount;
				try
				{
					fileSize = br.ReadUInt32();
					ushort headerVer = br.ReadUInt16();
					ushort typeVer = br.ReadUInt16();
					blockOffset = br.ReadUInt32();
					blockCount = br.ReadUInt32();
					
					// Validate header values
					if (blockCount > 1000 || blockOffset > bytes.Length)
					{
						Log.Warning($"[HLA] Invalid header: blockCount={blockCount}, blockOffset={blockOffset}, fileSize={bytes.Length}");
						return CreatePlaceholderTriangle();
					}
				}
				catch (Exception ex)
				{
					Log.Warning($"[HLA] Failed to read resource header: {ex.Message}");
					return CreatePlaceholderTriangle();
				}

                var blocks = new List<(uint type, uint offset, uint size)>();
                // Move to block table: relative from here minus 8 (as per VRF)
                ms.Position += blockOffset - 8;
				for ( int i = 0; i < blockCount; i++ )
				{
					uint blockType = br.ReadUInt32();
					long pos = ms.Position;
					uint rel = br.ReadUInt32();
					uint size = br.ReadUInt32();
					blocks.Add( (blockType, (uint)(pos + rel), size) );
					ms.Position = pos + 8;
				}
                if (EnableNonAnimLogs) Log.Info($"[HLA] Blocks: {string.Join(", ", blocks.Select(b => FourCC(b.type)+$"@0x{b.offset:X}+{b.size}"))}");

                // Check for PHYS block (physics data)
                uint PHYSc = (uint)('P' | ('H'<<8) | ('Y'<<16) | ('S'<<24));
                var physblk = blocks.FirstOrDefault( b => b.type == PHYSc );
                if ( physblk.size > 0 )
                {
                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found PHYS block @0x{physblk.offset:X} size={physblk.size}");
                    ms.Position = physblk.offset;
                    var physBlob = br.ReadBytes( (int)physblk.size );
                    if (EnableNonAnimLogs) Log.Info($"[HLA] PHYS block contains {physBlob.Length} bytes of physics data");
                    
                    // Try to parse PHYS block as KV3 (joints are likely here)
                    try
                    {
                        var physKV = BinaryKV3Lite.Parse( physBlob );
                        if ( physKV != null )
                        {
                            if (EnableNonAnimLogs) Log.Info($"[HLA] PHYS KV3 parsed; keys=[{string.Join(',', physKV.Keys)}]");
                            
                            // Debug: Log all PHYS keys and their types
                            foreach ( var kvp in physKV )
                            {
                                string valueType = kvp.Value?.GetType().Name ?? "null";
                                if ( kvp.Value is List<object> list )
                                {
                                    valueType = $"List<object>[{list.Count}]";
                                }
                                else if ( kvp.Value is Dictionary<string,object> dict )
                                {
                                    valueType = $"Dictionary[{dict.Count}]";
                                }
                                if (EnableNonAnimLogs) Log.Info($"[HLA] PHYS key: {kvp.Key} = {valueType}");
                            }
                            
                            // Store physics data for later processing (after builder is created)
                            if ( physKV.TryGetValue( "m_parts", out var partsObj ) && partsObj is List<object> partsList )
                            {
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Found {partsList.Count} physics parts - will create after skeleton");
                                physicsPartsData = (physKV, partsList);
                            }
                            
                            if ( physKV.TryGetValue( "m_joints", out var jointsObj ) && jointsObj is List<object> jointsList )
                            {
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Found {jointsList.Count} physics joints - will create after bodies");
                                physicsJointsData = (physKV, jointsList);
                            }
                        }
                        else
                        {
                            if (EnableNonAnimLogs) Log.Info($"[HLA] PHYS block is not KV3 format");
                        }
                    }
                    catch ( Exception ex )
                    {
                        Log.Warning($"[HLA] PHYS block KV3 parse failed: {ex.Message}");
                    }
                }

                // Find ALL VBIB or compatible variants (MBUF/TBUF share VBIB layout in VRF)
                uint VBIBc = (uint)('V' | ('B'<<8) | ('I'<<16) | ('B'<<24));
                uint MBUFc = (uint)('M' | ('B'<<8) | ('U'<<16) | ('F'<<24));
                uint TBUFc = (uint)('T' | ('B'<<8) | ('U'<<16) | ('F'<<24));
                var bufblks = blocks.Where( b => b.type == VBIBc || b.type == MBUFc || b.type == TBUFc ).ToList();
                if ( bufblks.Count == 0 )
                {
                    Log.Warning("[HLA] No VBIB/MBUF/TBUF blocks found; returning placeholder triangle");
                    return CreatePlaceholderTriangle();
                }
                
                if (EnableNonAnimLogs) Log.Info($"[HLA] Found {bufblks.Count} mesh blocks to process");
                
                // Initialize model builder and skeleton variables (used across all MBUF blocks)
                var builder = Model.Builder.WithName( base.Path );
                bool builtNamedSkeleton = false;
                int skeletonBoneCount = 0;
                Dictionary<string,int> boneNameToIndex = null;
                Dictionary<string,object> modelDecodeKeyKV = null;
                Vector3[] bindLocalPos = null;
                Rotation[] bindLocalRot = null;
                int[] skeletonParents = null;
                var blockTable = blocks.Select(b => (b.offset, b.size, FourCC(b.type))).ToList();
                
                // Process each MBUF/VBIB block to create separate meshes
                foreach ( var bufblk in bufblks )
                {
                    if (EnableNonAnimLogs) Log.Info($"[HLA] Processing {FourCC(bufblk.type)} block @0x{bufblk.offset:X} size={bufblk.size}");
                    
                    // Parse VBIB-style header (uncompressed path)
                ms.Position = bufblk.offset;
				uint vertexBufferOffset = br.ReadUInt32();
				uint vertexBufferCount = br.ReadUInt32();
				uint indexBufferOffset = br.ReadUInt32();
				uint indexBufferCount = br.ReadUInt32();
                if (EnableNonAnimLogs) Log.Info($"[HLA] VBIB hdr: vtxCount={vertexBufferCount} idxCount={indexBufferCount} vtxOfs=0x{vertexBufferOffset:X} idxOfs=0x{indexBufferOffset:X}");

                // Read first vertex buffer
                ms.Position = bufblk.offset + vertexBufferOffset;
                var (vertData, vertStride, vertCount, attributes, attributeFormats) = ReadVertexBuffer( br, ms );
				if ( vertData == null || vertCount == 0 || !attributes.ContainsKey("POSITION0") )
				{
					Log.Warning($"[HLA] Vertex buffer not readable (stride={vertStride}, count={vertCount}, hasPos={attributes.ContainsKey("POSITION0")}); skipping MBUF block");
					continue; // Skip this MBUF block instead of returning placeholder
				}

                // Read first index buffer
                ms.Position = bufblk.offset + 8 + indexBufferOffset; // +8 to account for vbo header fields
				var (indices, indexCount) = ReadIndexBuffer( br, ms );
				if ( indices == null || indexCount == 0 )
				{
					Log.Warning("[HLA] Index buffer missing; will sequentially triangulate");
				}

				// Build HLASkinnedVertex list with all available attributes
				var verts = new List<HLASkinnedVertex>( (int)vertCount );
				var bounds = new BBox { Mins = float.MaxValue, Maxs = float.MinValue };
				
				// Get attribute offsets and formats
				int posOffset = attributes.GetValueOrDefault("POSITION0", -1);
				int normalOffset = attributes.GetValueOrDefault("NORMAL0", -1);
				int tangentOffset = attributes.GetValueOrDefault("TANGENT0", -1);
				int texcoordOffset = attributes.GetValueOrDefault("TEXCOORD0", -1);
				int blendIndicesOffset = attributes.GetValueOrDefault("BLENDINDICES0", -1);
				int blendWeightsOffset = attributes.GetValueOrDefault("BLENDWEIGHT0", -1);
				
				// Get UV format for proper decoding - with safety checks
				uint uvFormat = 0x22; // Default to R16G16_FLOAT
				if (texcoordOffset >= 0 && attributeFormats != null && attributeFormats.ContainsKey("TEXCOORD0"))
				{
					uvFormat = attributeFormats["TEXCOORD0"];
					if (EnableNonAnimLogs) Log.Info($"[HLA] Detected UV format: 0x{uvFormat:X} for TEXCOORD0");
				}
				else
				{
					if (EnableNonAnimLogs) Log.Info($"[HLA] Using default UV format 0x{uvFormat:X}, texcoordOffset={texcoordOffset}");
				}
				
				for ( int i = 0; i < vertCount; i++ )
				{
					int baseOffset = i * vertStride;
					
					// Position (required)
					Vector3 position = Vector3.Zero;
					if (posOffset >= 0 && baseOffset + posOffset + 12 <= vertData.Length)
					{
						position = new Vector3(
							BitConverter.ToSingle(vertData, baseOffset + posOffset + 0),
							BitConverter.ToSingle(vertData, baseOffset + posOffset + 4),
							BitConverter.ToSingle(vertData, baseOffset + posOffset + 8)
						);
					}
					
					// Normal (optional)
					Vector3 normal = Vector3.Up;
					if (normalOffset >= 0 && baseOffset + normalOffset + 4 <= vertData.Length)
					{
						// NORMAL format 0x1C = R8G8B8A8_UNORM (Valve's compressed normal format)
						// First 2 bytes encode normal, next 2 bytes encode tangent
						byte x = vertData[baseOffset + normalOffset + 0];
						byte y = vertData[baseOffset + normalOffset + 1];
						normal = DecompressValveNormal(x, y);
					}
					
					// Tangent (optional)
					Vector3 tangent = Vector3.Forward;
					if (tangentOffset >= 0 && baseOffset + tangentOffset + 12 <= vertData.Length)
					{
						tangent = new Vector3(
							BitConverter.ToSingle(vertData, baseOffset + tangentOffset + 0),
							BitConverter.ToSingle(vertData, baseOffset + tangentOffset + 4),
							BitConverter.ToSingle(vertData, baseOffset + tangentOffset + 8)
						);
					}
					
				// Texture coordinates (optional) - VRF-style format detection with safety checks
					Vector2 texcoord = Vector2.Zero;
				if (texcoordOffset >= 0 && vertData != null && baseOffset + texcoordOffset + 8 <= vertData.Length)
				{
					try
					{
						switch (uvFormat)
						{
							case 34: // DXGI_FORMAT.R16G16_FLOAT
								{
						var u = (float)BitConverter.ToHalf(vertData, baseOffset + texcoordOffset + 0);
						var v = (float)BitConverter.ToHalf(vertData, baseOffset + texcoordOffset + 2);
						texcoord = new Vector2(u, v);
									break;
								}
							case 35: // DXGI_FORMAT.R16G16_UNORM
								{
									var u16 = BitConverter.ToUInt16(vertData, baseOffset + texcoordOffset + 0);
									var v16 = BitConverter.ToUInt16(vertData, baseOffset + texcoordOffset + 2);
									var u = u16 / 65535.0f;
									var v = v16 / 65535.0f;
									texcoord = new Vector2(u, v);
									break;
								}
							case 37: // DXGI_FORMAT.R16G16_SNORM
								{
									var s16_u = BitConverter.ToInt16(vertData, baseOffset + texcoordOffset + 0);
									var s16_v = BitConverter.ToInt16(vertData, baseOffset + texcoordOffset + 2);
									var u = s16_u / 32767.0f;
									var v = s16_v / 32767.0f;
									texcoord = new Vector2(u, v);
									break;
								}
							case 16: // DXGI_FORMAT.R32G32_FLOAT
								{
									if (baseOffset + texcoordOffset + 8 <= vertData.Length)
									{
										var u = BitConverter.ToSingle(vertData, baseOffset + texcoordOffset + 0);
										var v = BitConverter.ToSingle(vertData, baseOffset + texcoordOffset + 4);
										texcoord = new Vector2(u, v);
									}
									break;
								}
							default:
								// Fallback to half-float interpretation (most common)
								{
									var u = (float)BitConverter.ToHalf(vertData, baseOffset + texcoordOffset + 0);
									var v = (float)BitConverter.ToHalf(vertData, baseOffset + texcoordOffset + 2);
									texcoord = new Vector2(u, v);
									if (EnableNonAnimLogs && i < 3)
									{
										Log.Warning($"[HLA] Unknown UV format 0x{uvFormat:X}, using R16G16_FLOAT fallback");
									}
									break;
								}
						}
						
						// Debug log for first few vertices to verify UV range and values
						if (EnableNonAnimLogs && i < 5)
						{
							Log.Info($"[HLA] Vertex {i} UV: format=0x{uvFormat:X}, uv=({texcoord.x:F3}, {texcoord.y:F3}), offset={texcoordOffset}, bytes=[{vertData[baseOffset + texcoordOffset]},{vertData[baseOffset + texcoordOffset + 1]},{vertData[baseOffset + texcoordOffset + 2]},{vertData[baseOffset + texcoordOffset + 3]}]");
						}
					}
					catch (Exception ex)
					{
						if (EnableNonAnimLogs && i < 3)
						{
							Log.Warning($"[HLA] Error reading UV at vertex {i}: {ex.Message}");
						}
						texcoord = Vector2.Zero;
					}
				}
				else if (texcoordOffset >= 0 && EnableNonAnimLogs && i < 3)
				{
					Log.Warning($"[HLA] UV data out of bounds: baseOffset={baseOffset}, texcoordOffset={texcoordOffset}, vertData.Length={vertData?.Length}");
					}
					
					// Blend indices (required for skinning)
					Color32 blendIndices = new Color32(0, 0, 0, 0);
					if (blendIndicesOffset >= 0 && baseOffset + blendIndicesOffset + 4 <= vertData.Length)
					{
						// BLENDINDICES format 0x1E is 4 bytes (uint8 x4)
						blendIndices = new Color32(
							vertData[baseOffset + blendIndicesOffset + 0],
							vertData[baseOffset + blendIndicesOffset + 1],
							vertData[baseOffset + blendIndicesOffset + 2],
							vertData[baseOffset + blendIndicesOffset + 3]
						);
					}
					
					// Blend weights (required for skinning)
					Color32 blendWeights = new Color32(255, 0, 0, 0); // Default: full weight on bone 0
					if (blendWeightsOffset >= 0 && baseOffset + blendWeightsOffset + 4 <= vertData.Length)
					{
						// BLENDWEIGHT format 0x1C = R8G8B8A8_UNORM (4 bytes, already normalized)
						blendWeights = new Color32(
							vertData[baseOffset + blendWeightsOffset + 0],
							vertData[baseOffset + blendWeightsOffset + 1],
							vertData[baseOffset + blendWeightsOffset + 2],
							vertData[baseOffset + blendWeightsOffset + 3]
						);
					}
					
					verts.Add(new HLASkinnedVertex(position, normal, tangent, texcoord, blendIndices, blendWeights));
					bounds = bounds.AddPoint(position);
				}
				if (EnableNonAnimLogs) Log.Info($"[HLA] Parsed {verts.Count} vertices, stride={vertStride}");

				// Find all materials for this model
				var materialPaths = FindAllMeshMaterials( ms, blockTable );
				
				// For now, use the first material (we'll enhance this later for multiple meshes)
				var primaryMaterialPath = materialPaths.FirstOrDefault();
				var material = LoadHLAMaterial( primaryMaterialPath, Host );
				
				if ( materialPaths.Count > 1 )
				{
					if (EnableNonAnimLogs) Log.Info($"[HLA] Model has {materialPaths.Count} materials, using primary: {primaryMaterialPath}");
					if (EnableNonAnimLogs) Log.Info($"[HLA] Other materials: {string.Join(", ", materialPaths.Skip(1))}");
				}
                var mesh = new Mesh( material );
				mesh.Bounds = bounds;
				mesh.CreateVertexBuffer<HLASkinnedVertex>( verts.Count, HLASkinnedVertex.Layout, verts );
				int[] finalIndices;
				if ( indices != null && indices.Length >= 3 )
				{
					// Clamp to multiple of 3
					finalIndices = indices.Take( (indices.Length/3)*3 ).ToArray();
				}
				else
				{
					int triCount = (verts.Count/3);
					finalIndices = Enumerable.Range(0, triCount*3).ToArray();
					Log.Warning($"[HLA] Built {triCount} fallback triangles from consecutive vertices");
				}
				mesh.CreateIndexBuffer( finalIndices.Length, finalIndices );

                // Builder and skeleton variables already declared above

                // Build skeleton scanning both DATA and MDAT; also support MDAT bone object layout (m_skeleton.m_bones[])
                foreach ( var blk in blocks )
                {
                    var four = FourCC( blk.type );
                    if ( four != "DATA" && four != "MDAT" ) continue;
                    if ( blk.size == 0 ) continue;
                    if (EnableNonAnimLogs) Log.Info($"[HLA] Skeleton scan: {four} @0x{blk.offset:X} size={blk.size}");
                    try
                    {
                        ms.Position = blk.offset;
                        var blob = br.ReadBytes( (int)blk.size );

                        // Try full BinaryKV3 first (handles compressed DATA/MDAT)
                        try
                        {
                            var kv = BinaryKV3Lite.Parse( blob );
                            if ( kv != null )
                            {
                                if (EnableNonAnimLogs) Log.Info($"[HLA] KV3 parsed for {four}; keys=[{string.Join(',', kv.Keys)}]");
                                
                                // Debug: Log all top-level keys and their types
                                foreach ( var kvp in kv )
                                {
                                    string valueType = kvp.Value?.GetType().Name ?? "null";
                                    if ( kvp.Value is List<object> list )
                                    {
                                        valueType = $"List<object>[{list.Count}]";
                                    }
                                    else if ( kvp.Value is Dictionary<string,object> dict )
                                    {
                                        valueType = $"Dictionary[{dict.Count}]";
                                    }
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] KV3 key: {kvp.Key} = {valueType}");
                                }
                                
                                // Check for bodygroup data
                                if (kv.ContainsKey("m_meshGroups") || kv.ContainsKey("m_refMeshGroupMasks"))
                                {
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found bodygroup data: m_meshGroups={kv.ContainsKey("m_meshGroups")} m_refMeshGroupMasks={kv.ContainsKey("m_refMeshGroupMasks")}");
                                    
                                    // Parse bodygroups for later use with meshes
                                    var meshGroups = GetStringArray(kv, "m_meshGroups");
                                    var meshGroupMasks = GetLongArray(kv, "m_refMeshGroupMasks");
                                    if (meshGroups != null && meshGroupMasks != null)
                                    {
                                        if (EnableNonAnimLogs) Log.Info($"[HLA] Bodygroups: {meshGroups.Length} groups, {meshGroupMasks.Length} masks");
                                        foreach (var groupName in meshGroups)
                                        {
                                            if (EnableNonAnimLogs) Log.Info($"[HLA] Group: {groupName}");
                                        }
                                        // TODO: Store this data for use when creating meshes
                                    }
                                }
                                
                                // Check for bone constraint/joint data
                                if (kv.ContainsKey("BoneConstraintList"))
                                {
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found bone constraints!");
                                    var boneConstraints = GetObjectArray(kv, "BoneConstraintList");
                                    if (boneConstraints != null)
                                    {
                                        if (EnableNonAnimLogs) Log.Info($"[HLA] BoneConstraintList: {boneConstraints.Length} constraints");
                                        foreach (var constraint in boneConstraints)
                                        {
                                            if (constraint is Dictionary<string, object> constraintDict)
                                            {
                                                var className = GetString(constraintDict, "_class");
                                                if (EnableNonAnimLogs) Log.Info($"[HLA] Constraint: {className}");
                                            }
                                        }
                                        // TODO: Parse and implement bone constraints
                                    }
                                }
                                // DATA layout: m_modelSkeleton fields
                                if ( kv.TryGetValue( "m_modelSkeleton", out var skObj ) && skObj is Dictionary<string,object> sk )
                                {
                                    var names = GetStringArray( sk, "m_boneName" );
                                    var parents = GetIntArray( sk, "m_nParent" );
                                    var pos = GetVec3Array( sk, "m_bonePosParent" );
                                    var rot = GetQuatArray( sk, "m_boneRotParent" );
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] m_modelSkeleton present: names={(names?.Length ?? -1)} parents={(parents?.Length ?? -1)} pos={(pos?.Length ?? -1)} rot={(rot?.Length ?? -1)}");
                                    if ( names != null && parents != null && pos != null && rot != null && names.Length == parents.Length && names.Length == pos.Length && names.Length == rot.Length )
                                    {
                                        boneNameToIndex = new Dictionary<string,int>( names.Length, StringComparer.OrdinalIgnoreCase );
                                        for ( int i = 0; i < names.Length; i++ ) boneNameToIndex[names[i]] = i;
							bindLocalPos = pos;
							bindLocalRot = rot;
							skeletonParents = parents;
                                        AddBonesWorldChained( builder, names, parents, pos, rot );
                                        if (EnableNonAnimLogs) Log.Info($"[HLA] Added skeleton from {four}/BinaryKV3 (m_modelSkeleton): {names.Length}");
                                        builtNamedSkeleton = true;
                                        skeletonBoneCount = names.Length;
                                        break;
                                    }
                                    else
                                    {
                                        Log.Warning($"[HLA] m_modelSkeleton arrays invalid or mismatched");
                                    }
                                }

                                // Capture decode key if present
                                if ( kv.TryGetValue( "m_decodeKey", out var decodeObj ) && decodeObj is Dictionary<string,object> dk )
                                {
                                    modelDecodeKeyKV = dk;
                                }

                                // MDAT object layout: m_skeleton.m_bones[] with inv bind poses
                                if ( kv.TryGetValue( "m_skeleton", out var sk2Obj ) && sk2Obj is Dictionary<string,object> sk2 )
                                {
                                    if ( sk2.TryGetValue( "m_bones", out var bonesObj ) && bonesObj is List<object> bonesList && bonesList.Count > 0 )
                                    {
                                        if (EnableNonAnimLogs) Log.Info($"[HLA] m_skeleton.m_bones count={bonesList.Count}");
                                        boneNameToIndex = new Dictionary<string,int>( bonesList.Count, StringComparer.OrdinalIgnoreCase );
							// First pass: assign indices
                                        foreach ( var o in bonesList )
                                        {
                                            if ( o is not Dictionary<string,object> bo ) continue;
                                            string name = GetString( bo, "m_boneName" );
                                            string parentName = GetString( bo, "m_parentName" );
                                            var inv = GetFloatArray( bo, "m_invBindPose" );
                                            if ( string.IsNullOrEmpty(name) || inv == null || inv.Length < 12 ) continue;

                                            // Invert 3x4 affine: R^-1 = R^T, t' = -R^T * t
                                            float r00 = inv[0], r01 = inv[1], r02 = inv[2], t0 = inv[3];
                                            float r10 = inv[4], r11 = inv[5], r12 = inv[6], t1 = inv[7];
                                            float r20 = inv[8], r21 = inv[9], r22 = inv[10], t2 = inv[11];
                                            float R00 = r00, R01 = r10, R02 = r20;
                                            float R10 = r01, R11 = r11, R12 = r21;
                                            float R20 = r02, R21 = r12, R22 = r22;
                                            var posW = new Vector3( -(R00*t0 + R01*t1 + R02*t2), -(R10*t0 + R11*t1 + R12*t2), -(R20*t0 + R21*t1 + R22*t2) );
                                            var rotW = RotationFromMatrix( R00, R01, R02, R10, R11, R12, R20, R21, R22 );
                                            builder.AddBone( name, posW, rotW, string.IsNullOrEmpty(parentName) ? null : parentName );
                                            boneNameToIndex[name] = boneNameToIndex.Count;
                                        }
							// Second pass: build parents array
							skeletonParents = new int[bonesList.Count];
							for ( int bi = 0; bi < bonesList.Count; bi++ )
							{
								if ( bonesList[bi] is not Dictionary<string,object> bo2 ) { skeletonParents[bi] = -1; continue; }
								string name2 = GetString( bo2, "m_boneName" );
								string parent2 = GetString( bo2, "m_parentName" );
								int idx2 = boneNameToIndex.TryGetValue(name2, out var ix) ? ix : bi;
								skeletonParents[idx2] = (parent2 != null && boneNameToIndex.TryGetValue(parent2, out var pix)) ? pix : -1;
							}
                                        if (EnableNonAnimLogs) Log.Info($"[HLA] Added skeleton from {four}/BinaryKV3 (m_skeleton.m_bones)");
                                        builtNamedSkeleton = true;
                                        skeletonBoneCount = bonesList.Count;
                                    }
                                }

                                // Parse physics joints (like NS2 does) 
                                if ( kv.TryGetValue( "m_joints", out var jointsObj ) && jointsObj is List<object> jointsList )
                                {
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found {jointsList.Count} physics joints");
                                    foreach ( var jointObj in jointsList )
                                    {
                                        if ( jointObj is Dictionary<string,object> joint )
                                        {
                                            var jointType = GetInt( joint, "m_nType" );
                                            var body1 = GetInt( joint, "m_nBody1" );
                                            var body2 = GetInt( joint, "m_nBody2" );
                                            
                                            // Parse joint frames (position + orientation as 7-float arrays: [x,y,z,qx,qy,qz,qw])
                                            var frame1Array = GetFloatArray( joint, "m_Frame1" );
                                            var frame2Array = GetFloatArray( joint, "m_Frame2" );
                                            
                                            if ( frame1Array?.Length >= 7 && frame2Array?.Length >= 7 )
                                            {
                                                var frame1 = new Transform(
                                                    new Vector3(frame1Array[0], frame1Array[1], frame1Array[2]),
                                                    new Rotation(frame1Array[3], frame1Array[4], frame1Array[5], frame1Array[6])
                                                );
                                                var frame2 = new Transform(
                                                    new Vector3(frame2Array[0], frame2Array[1], frame2Array[2]),
                                                    new Rotation(frame2Array[3], frame2Array[4], frame2Array[5], frame2Array[6])
                                                );
                                                
                                                // Parse swing and twist limits
                                                float swingMax = 30.0f; // Default
                                                float twistMin = -15.0f; // Default  
                                                float twistMax = 15.0f; // Default
                                                
                                                if ( joint.TryGetValue( "m_SwingLimit", out var swingLimitObj ) && swingLimitObj is Dictionary<string,object> swingLimit )
                                                {
                                                    var swingMaxRad = GetFloat( swingLimit, "m_flMax" );
                                                    if ( swingMaxRad.HasValue ) swingMax = swingMaxRad.Value * (180.0f / MathF.PI); // Convert to degrees
                                                }
                                                
                                                if ( joint.TryGetValue( "m_TwistLimit", out var twistLimitObj ) && twistLimitObj is Dictionary<string,object> twistLimit )
                                                {
                                                    var twistMinRad = GetFloat( twistLimit, "m_flMin" );
                                                    var twistMaxRad = GetFloat( twistLimit, "m_flMax" );
                                                    if ( twistMinRad.HasValue ) twistMin = twistMinRad.Value * (180.0f / MathF.PI);
                                                    if ( twistMaxRad.HasValue ) twistMax = twistMaxRad.Value * (180.0f / MathF.PI);
                                                }
                                                
                                                if (EnableNonAnimLogs) Log.Info($"[HLA] Joint type={jointType} body1={body1} body2={body2} swing={swingMax:F1}° twist=[{twistMin:F1}°,{twistMax:F1}°]");
                                                
                                                // TODO: Store joint data for later use when creating physics bodies
                                                // We need to parse the PHYS block first to get the actual physics bodies
                                                // Then call: modelBuilder.AddBallJoint(body1Id, body2Id, frame1, frame2, false)
                                                //                        .WithSwingLimit(swingMax).WithTwistLimit(twistMin, twistMax);
                                            }
                                        }
                                    }
                                }

                                // Parse constraints for animation (different from physics joints)
                                if ( kv.TryGetValue( "m_constraints", out var constraintsObj ) && constraintsObj is List<object> constraintsList )
                                {
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found {constraintsList.Count} animation constraints");
                                    foreach ( var constraintObj in constraintsList )
                                    {
                                        if ( constraintObj is Dictionary<string,object> constraint )
                                        {
                                            var className = GetString( constraint, "_class" );
                                            if (EnableNonAnimLogs) Log.Info($"[HLA] Animation constraint: {className}");
                                        }
                                    }
                                }

                                // Parse hitboxes for potential physics bodies
                                if ( kv.TryGetValue( "m_hitboxsets", out var hitboxObj ) && hitboxObj is List<object> hitboxList )
                                {
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found {hitboxList.Count} hitbox sets");
                                    foreach ( var hitboxSetObj in hitboxList )
                                    {
                                        if ( hitboxSetObj is Dictionary<string,object> hitboxSet )
                                        {
                                            var hitboxName = GetString( hitboxSet, "m_name" );
                                            if ( hitboxSet.TryGetValue( "m_hitboxes", out var hboxesObj ) && hboxesObj is List<object> hboxes )
                                            {
                                                if (EnableNonAnimLogs) Log.Info($"[HLA] Hitbox set '{hitboxName}': {hboxes.Count} hitboxes");
                                                // TODO: Create physics bodies from hitboxes
                                            }
                                        }
                                    }
                                }
                                
                                if ( builtNamedSkeleton ) break;
                            }
                        }
                        catch ( Exception kvex )
                        {
                            // Fallback: try naive text scan for uncompressed cases
                            var txt = Encoding.UTF8.GetString( blob );
                            Log.Warning($"[HLA] KV3 parse failed for {four}: {kvex.Message}; trying text scan (len={txt.Length})");

                            var names = ExtractStringArray( txt, "m_boneName" );
                            var parents = ExtractIntArray( txt, "m_nParent" );
                            var pos = ExtractVec3Array( txt, "m_bonePosParent" );
                            var rot = ExtractQuatArray( txt, "m_boneRotParent" );
                            if ( names != null && parents != null && pos != null && rot != null && names.Length == parents.Length && names.Length == pos.Length && names.Length == rot.Length )
                            {
                                AddBonesWorldChained( builder, names, parents, pos, rot );
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Added skeleton from {four} (array layout/text)");
                                builtNamedSkeleton = true;
                                break;
                            }

                            var bones = ExtractBoneObjects( txt );
                            if ( bones != null && bones.Count > 0 )
                            {
                                foreach ( var b in bones )
                                {
                                    var m = b.InvBind;
                                    var r00 = m[0]; var r01 = m[1]; var r02 = m[2]; var t0 = m[3];
                                    var r10 = m[4]; var r11 = m[5]; var r12 = m[6]; var t1 = m[7];
                                    var r20 = m[8]; var r21 = m[9]; var r22 = m[10]; var t2 = m[11];
                                    var R00 = r00; var R01 = r10; var R02 = r20;
                                    var R10 = r01; var R11 = r11; var R12 = r21;
                                    var R20 = r02; var R21 = r12; var R22 = r22;
                                    var px = -(R00 * t0 + R01 * t1 + R02 * t2);
                                    var py = -(R10 * t0 + R11 * t1 + R12 * t2);
                                    var pz = -(R20 * t0 + R21 * t1 + R22 * t2);
                                    var posW = new Vector3( px, py, pz );
                                    var rotW = RotationFromMatrix( R00, R01, R02, R10, R11, R12, R20, R21, R22 );
                                    builder.AddBone( b.Name, posW, rotW, b.ParentName.Length > 0 ? b.ParentName : null );
                                }
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Added skeleton from {four} (MDAT objects/text): {bones.Count}");
                                builtNamedSkeleton = true;
                                    skeletonBoneCount = bones.Count;
                                break;
                            }
                        }
                    }
                    catch { }
                }

                // If no named skeleton was found, add identity bones from blend indices
                if ( !builtNamedSkeleton && attributes.ContainsKey("BLENDINDICES0") && vertData != null && vertCount > 0 )
                {
                    int maxBone = 0;
                    int minBone = int.MaxValue;
                    int samples = 0;
                    int step = vertStride;
                    int ofs = attributes["BLENDINDICES0"];
                    for ( int i = 0; i < vertCount; i++ )
                    {
                        int baseOfs = ofs + i * step;
                        if ( baseOfs + 4 <= vertData.Length )
                        {
                            int b0 = vertData[baseOfs + 0];
                            int b1 = vertData[baseOfs + 1];
                            int b2 = vertData[baseOfs + 2];
                            int b3 = vertData[baseOfs + 3];
                            samples++;
                            if ( b0 > maxBone ) maxBone = b0;
                            if ( b1 > maxBone ) maxBone = b1;
                            if ( b2 > maxBone ) maxBone = b2;
                            if ( b3 > maxBone ) maxBone = b3;
                            if ( b0 < minBone ) minBone = b0;
                            if ( b1 < minBone ) minBone = b1;
                            if ( b2 < minBone ) minBone = b2;
                            if ( b3 < minBone ) minBone = b3;
                        }
                    }
					boneNameToIndex = new Dictionary<string,int>( maxBone + 1, StringComparer.OrdinalIgnoreCase );
                    for ( int b = 0; b <= maxBone; b++ )
                    {
                        builder.AddBone( $"bone{b}", Vector3.Zero, Rotation.Identity, null );
                        boneNameToIndex[$"bone{b}"] = b;
                    }
                    Log.Warning($"[HLA] Fallback identity bones: stride={vertStride} blendOfs={ofs} samples={samples} minBone={minBone} maxBone={maxBone}");
                    skeletonBoneCount = maxBone + 1;
					skeletonParents = new int[skeletonBoneCount];
					for ( int b = 0; b < skeletonBoneCount; b++ ) skeletonParents[b] = -1;
                }

                // Add mesh with safety checks
                try
                {
                    if (mesh != null && mesh.VertexCount > 0 && mesh.IndexCount > 0)
                    {
                        builder.AddMesh( mesh );
                        int triangleCount = mesh.IndexCount / 3;
                        if (EnableNonAnimLogs) Log.Info($"[HLA] Added mesh with {mesh.VertexCount} vertices, {triangleCount} triangles");
                    }
                    else
                    {
                        int triangleCount = (mesh?.IndexCount ?? 0) / 3;
                        Log.Warning($"[HLA] Invalid mesh: vertices={mesh?.VertexCount}, triangles={triangleCount}");
                    }
                }
                catch (Exception ex)
                {
                    Log.Warning($"[HLA] Failed to add mesh: {ex.Message}");
                }
                } // End of foreach MBUF block
                
                // Create physics bodies and joints if we have physics data (once for all meshes)
                // Add material groups to the model
                try
                {
                    AddMaterialGroups( builder, ms, blockTable, Host );
                }
                catch (Exception ex)
                {
                    Log.Warning($"[HLA] Failed to add material groups: {ex.Message}");
                }

                CreatePhysicsBodies( builder, physicsPartsData, physicsJointsData );

                // Attempt to add embedded animations from ANIM/ASEQ/AGRP blocks, then fall back to external refs
                try
                {
                    int added = 0;
                    if ( skeletonBoneCount > 0 )
                    {
							added = TryAddAnimationsFromEmbeddedBlocks( builder, blocks, ms, br, skeletonBoneCount, boneNameToIndex, bindLocalPos, bindLocalRot, modelDecodeKeyKV, skeletonParents );
                        if ( added == 0 )
                        {
                            AddAnimationsForModel( builder, _vpkPath, skeletonBoneCount );
                        }
                    }
                    else
                    {
                        Log.Info("[HLA] Skipping animation import (no skeleton built)");
                    }
                }
                catch ( Exception animEx )
                {
                    Log.Warning($"[HLA] Animation import failed: {animEx.Message}");
                }
                
                return builder.Create();
			}
			catch ( Exception e )
			{
				Log.Warning( $"[HLA] Model load failed for {_vpkPath}: {e}" );
				return CreatePlaceholderTriangle();
			}
		}

		private static (byte[] data, int stride, int count, Dictionary<string, int> attributes, Dictionary<string, uint> attributeFormats) ReadVertexBuffer( BinaryReader br, Stream s )
		{
			uint elementCount = br.ReadUInt32();
			int sizeField = br.ReadInt32();
			int elementSizeBytes = sizeField & 0x3FFFFFF;
			long refA = s.Position;
			uint attributeOffset = br.ReadUInt32();
			uint attributeCount = br.ReadUInt32();
			long refB = s.Position;
			uint dataOffset = br.ReadUInt32();
			int totalSize = br.ReadInt32();
            if (EnableNonAnimLogs) Log.Info($"[HLA] VB: count={elementCount} stride={elementSizeBytes} attrCount={attributeCount} dataSize={totalSize}");

			// Read attributes
			s.Position = refA + attributeOffset;
			var attributes = new Dictionary<string, int>();
			var attributeFormats = new Dictionary<string, uint>();
			for ( int i = 0; i < attributeCount; i++ )
			{
				long attrStart = s.Position;
				string semantic = ReadFixedString( br, 32 );
				int semanticIndex = br.ReadInt32();
				uint format = br.ReadUInt32();
				uint offset = br.ReadUInt32();
				int slot = br.ReadInt32();
				uint slotType = br.ReadUInt32();
				int stepRate = br.ReadInt32();
				
				// Store all relevant attributes and formats
				string key = semantic + semanticIndex.ToString();
				attributes[key] = (int)offset;
				attributeFormats[key] = format;
				
                if (EnableNonAnimLogs) Log.Info($"[HLA] Attr {i}: {semantic}{semanticIndex} fmt=0x{format:X} ofs={offset} slot={slot} type={slotType} step={stepRate}");
			}

                // Read data
                s.Position = refB + dataOffset;
                var data = br.ReadBytes( totalSize );
                int expected = (int)elementCount * elementSizeBytes;
                if ( data.Length == expected )
                {
				return (data, elementSizeBytes, (int)elementCount, attributes, attributeFormats);
                }
                // Try meshoptimizer decode path (matches VRF): data is compressed to smaller than decompressed
                try
                {
                    var decoded = MeshOptimizerVertexDecoder.DecodeVertexBuffer( (int)elementCount, elementSizeBytes, data );
                    if ( decoded != null && decoded.Length == expected )
                    {
                        if (EnableNonAnimLogs) Log.Info($"[HLA] Decoded meshopt vertex buffer to {decoded.Length} bytes");
					return (decoded, elementSizeBytes, (int)elementCount, attributes, attributeFormats);
                    }
                }
                catch ( System.Exception ex )
                {
                    Log.Warning($"[HLA] meshopt vertex decode failed: {ex.Message}");
                }
			return (null, 0, 0, new Dictionary<string, int>(), new Dictionary<string, uint>());
		}

		private static (int[] indices, int count) ReadIndexBuffer( BinaryReader br, Stream s )
		{
			uint elementCount = br.ReadUInt32();
			int sizeField = br.ReadInt32();
			int elementSizeBytes = sizeField & 0x3FFFFFF;
			long refA = s.Position;
			uint attributeOffset = br.ReadUInt32();
			uint attributeCount = br.ReadUInt32();
			long refB = s.Position;
			uint dataOffset = br.ReadUInt32();
			int totalSize = br.ReadInt32();
            if (EnableNonAnimLogs) Log.Info($"[HLA] IB: count={elementCount} elemSize={elementSizeBytes} dataSize={totalSize}");

            // Index buffers have no attributes; jump to data
            s.Position = refB + dataOffset;
            var data = br.ReadBytes( totalSize );
            int expected = (int)elementCount * elementSizeBytes;
            byte[] decodedIdx = null;
            if ( data.Length == expected )
            {
                decodedIdx = data;
            }
            else
            {
                try
                {
                    decodedIdx = MeshOptimizerIndexDecoder.DecodeIndexBuffer( (int)elementCount, elementSizeBytes, data );
                    if (EnableNonAnimLogs) Log.Info($"[HLA] Decoded meshopt index buffer to {decodedIdx.Length} bytes");
                }
                catch ( System.Exception ex )
                {
                    Log.Warning($"[HLA] meshopt index decode failed: {ex.Message}");
                    return (null, 0);
                }
            }
            var indices = new int[elementCount];
            if ( elementSizeBytes == 2 )
            {
                for ( int i = 0; i < elementCount; i++ )
                {
                    indices[i] = BitConverter.ToUInt16( decodedIdx, i * 2 );
                }
            }
            else
            {
                for ( int i = 0; i < elementCount; i++ )
                {
                    indices[i] = BitConverter.ToInt32( decodedIdx, i * 4 );
                }
            }
            return (indices, (int)elementCount);
		}

		private static string ReadFixedString( BinaryReader br, int len )
		{
			var bytes = br.ReadBytes( len );
			int n = Array.IndexOf( bytes, (byte)0 );
			if ( n < 0 ) n = bytes.Length;
			return Encoding.UTF8.GetString( bytes, 0, n ).ToUpperInvariant();
		}

		// Extremely lightweight KV3 text helpers for arrays we need
		private static string[] ExtractStringArray( string txt, string key )
		{
			int k = txt.IndexOf( key, StringComparison.Ordinal );
			if ( k < 0 ) return null;
			int lb = txt.IndexOf( '[', k );
			int rb = txt.IndexOf( ']', lb + 1 );
			if ( lb < 0 || rb < 0 ) return null;
			var inner = txt.Substring( lb + 1, rb - lb - 1 );
			var parts = inner.Split( ',', StringSplitOptions.RemoveEmptyEntries );
			for ( int i = 0; i < parts.Length; i++ )
			{
				var p = parts[i].Trim();
				if ( p.Length >= 2 && p[0] == '"' && p[^1] == '"' ) p = p.Substring( 1, p.Length - 2 );
				parts[i] = p;
			}
			return parts;
		}

		private static int[] ExtractIntArray( string txt, string key )
		{
			int k = txt.IndexOf( key, StringComparison.Ordinal );
			if ( k < 0 ) return null;
			int lb = txt.IndexOf( '[', k );
			int rb = txt.IndexOf( ']', lb + 1 );
			if ( lb < 0 || rb < 0 ) return null;
			var inner = txt.Substring( lb + 1, rb - lb - 1 );
			var parts = inner.Split( ',', StringSplitOptions.RemoveEmptyEntries );
			var arr = new int[parts.Length];
			for ( int i = 0; i < parts.Length; i++ ) arr[i] = int.Parse( parts[i] );
			return arr;
		}

		private static Vector3[] ExtractVec3Array( string txt, string key )
		{
			int k = txt.IndexOf( key, StringComparison.Ordinal );
			if ( k < 0 ) return null;
			int lb = txt.IndexOf( '[', k );
			int rb = txt.IndexOf( ']', lb + 1 );
			if ( lb < 0 || rb < 0 ) return null;
			var inner = txt.Substring( lb + 1, rb - lb - 1 );
			var parts = inner.Split( ']', StringSplitOptions.RemoveEmptyEntries );
			var list = new System.Collections.Generic.List<Vector3>();
			foreach ( var p in parts )
			{
				int lb2 = p.IndexOf( '[' );
				if ( lb2 < 0 ) continue;
				var v = p.Substring( lb2 + 1 ).Split( ',', StringSplitOptions.RemoveEmptyEntries );
				if ( v.Length < 3 ) continue;
				list.Add( new Vector3( float.Parse( v[0] ), float.Parse( v[1] ), float.Parse( v[2] ) ) );
			}
			return list.ToArray();
		}

		private static Rotation[] ExtractQuatArray( string txt, string key )
		{
			int k = txt.IndexOf( key, StringComparison.Ordinal );
			if ( k < 0 ) return null;
			int lb = txt.IndexOf( '[', k );
			int rb = txt.IndexOf( ']', lb + 1 );
			if ( lb < 0 || rb < 0 ) return null;
			var inner = txt.Substring( lb + 1, rb - lb - 1 );
			var parts = inner.Split( ']', StringSplitOptions.RemoveEmptyEntries );
			var list = new System.Collections.Generic.List<Rotation>();
			foreach ( var p in parts )
			{
				int lb2 = p.IndexOf( '[' );
				if ( lb2 < 0 ) continue;
				var v = p.Substring( lb2 + 1 ).Split( ',', StringSplitOptions.RemoveEmptyEntries );
				if ( v.Length < 4 ) continue;
				list.Add( new Rotation( float.Parse( v[0] ), float.Parse( v[1] ), float.Parse( v[2] ), float.Parse( v[3] ) ) );
			}
			return list.ToArray();
		}

		private static void AddBonesWorldChained( ModelBuilder builder, string[] names, int[] parents, Vector3[] pos, Rotation[] rot )
		{
			var worldPos = new Vector3[names.Length];
			var worldRot = new Rotation[names.Length];
			for ( int i = 0; i < names.Length; i++ )
			{
				int parent = parents[i];
				if ( parent >= 0 && parent < i )
				{
					var p = worldPos[parent];
					var r = worldRot[parent];
					worldPos[i] = p + r * pos[i];
					worldRot[i] = (r * rot[i]).Normal;
				}
				else
				{
					worldPos[i] = pos[i];
					worldRot[i] = rot[i].Normal;
				}
			}
			for ( int i = 0; i < names.Length; i++ )
			{
				string parentName = null;
				int parent = parents[i];
				if ( parent >= 0 && parent < names.Length ) parentName = names[parent];
				builder.AddBone( names[i], worldPos[i], worldRot[i], parentName );
			}
		}

		private struct MdatBone { public string Name; public string ParentName; public float[] InvBind; }

		private static System.Collections.Generic.List<MdatBone> ExtractBoneObjects( string txt )
		{
			var bones = new System.Collections.Generic.List<MdatBone>();
			int sk = txt.IndexOf( "m_skeleton", StringComparison.Ordinal );
			if ( sk < 0 ) return null;
			int bk = txt.IndexOf( "m_bones", sk, StringComparison.Ordinal );
			if ( bk < 0 ) return null;
			int lb = txt.IndexOf( '[', bk );
			int pos = lb + 1;
			while ( pos > 0 && pos < txt.Length )
			{
				int objStart = txt.IndexOf( '{', pos );
				if ( objStart < 0 ) break;
				int objEnd = txt.IndexOf( '}', objStart + 1 );
				if ( objEnd < 0 ) break;
				var obj = txt.Substring( objStart, objEnd - objStart + 1 );
				string name = ExtractFirstString( obj, "m_boneName" );
				string parent = ExtractFirstString( obj, "m_parentName" );
				float[] inv = ExtractFloatArray12( obj, "m_invBindPose" );
				if ( name != null && inv != null ) bones.Add( new MdatBone{ Name=name, ParentName=parent??string.Empty, InvBind=inv } );
				pos = objEnd + 1;
				if ( txt[pos] == ']' ) break;
			}
			return bones;
		}

		private static string ExtractFirstString( string txt, string key )
		{
			int k = txt.IndexOf( key, StringComparison.Ordinal );
			if ( k < 0 ) return null;
			int q0 = txt.IndexOf( '"', k );
			int q1 = q0 >= 0 ? txt.IndexOf( '"', q0 + 1 ) : -1;
			if ( q0 < 0 || q1 < 0 ) return null;
			return txt.Substring( q0 + 1, q1 - q0 - 1 );
		}

		private static float[] ExtractFloatArray12( string txt, string key )
		{
			int k = txt.IndexOf( key, StringComparison.Ordinal );
			if ( k < 0 ) return null;
			int lb = txt.IndexOf( '[', k );
			int rb = txt.IndexOf( ']', lb + 1 );
			if ( lb < 0 || rb < 0 ) return null;
			var inner = txt.Substring( lb + 1, rb - lb - 1 );
			var parts = inner.Split( ',', StringSplitOptions.RemoveEmptyEntries );
			if ( parts.Length < 12 ) return null;
			var arr = new float[12];
			for ( int i = 0; i < 12; i++ ) arr[i] = float.Parse( parts[i] );
			return arr;
		}

		private static Rotation RotationFromMatrix( float R00, float R01, float R02, float R10, float R11, float R12, float R20, float R21, float R22 )
		{
			// Convert 3x3 to quaternion
			float trace = R00 + R11 + R22;
			float qw, qx, qy, qz;
			if ( trace > 0.0f )
			{
				float s = MathF.Sqrt( trace + 1.0f ) * 2f;
				qw = 0.25f * s;
				qx = (R21 - R12) / s;
				qy = (R02 - R20) / s;
				qz = (R10 - R01) / s;
			}
			else if ( R00 > R11 && R00 > R22 )
			{
				float s = MathF.Sqrt( 1.0f + R00 - R11 - R22 ) * 2f;
				qw = (R21 - R12) / s;
				qx = 0.25f * s;
				qy = (R01 + R10) / s;
				qz = (R02 + R20) / s;
			}
			else if ( R11 > R22 )
			{
				float s = MathF.Sqrt( 1.0f + R11 - R00 - R22 ) * 2f;
				qw = (R02 - R20) / s;
				qx = (R01 + R10) / s;
				qy = 0.25f * s;
				qz = (R12 + R21) / s;
			}
			else
			{
				float s = MathF.Sqrt( 1.0f + R22 - R00 - R11 ) * 2f;
				qw = (R10 - R01) / s;
				qx = (R02 + R20) / s;
				qy = (R12 + R21) / s;
				qz = 0.25f * s;
			}
			return new Rotation( qx, qy, qz, qw ).Normal;
		}

		// Helpers to pull arrays from BinaryKV3Lite object graph
		private static string GetString( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is string s ) return s;
			return null;
		}

		private static string[] GetStringArray( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				var arr = new string[list.Count];
				for ( int i = 0; i < arr.Length; i++ ) arr[i] = list[i] as string ?? string.Empty;
				return arr;
			}
			return null;
		}

		private static int[] GetIntArray( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				var arr = new int[list.Count];
				for ( int i = 0; i < arr.Length; i++ ) arr[i] = Convert.ToInt32( list[i] );
				return arr;
			}
			return null;
		}

		private static long[] GetLongArray( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				var arr = new long[list.Count];
				for ( int i = 0; i < arr.Length; i++ ) arr[i] = Convert.ToInt64( list[i] );
				return arr;
			}
			return null;
		}

		private static object[] GetObjectArray( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				return list.ToArray();
			}
			return null;
		}

		private static int? GetInt( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) )
			{
				return Convert.ToInt32( v );
			}
			return null;
		}

		private static float? GetFloat( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) )
			{
				return Convert.ToSingle( v );
			}
			return null;
		}

		private static Vector3[] GetVec3Array( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				var arr = new Vector3[list.Count];
				for ( int i = 0; i < arr.Length; i++ )
				{
					if ( list[i] is System.Collections.Generic.List<object> c && c.Count >= 3 )
					{
						arr[i] = new Vector3( Convert.ToSingle( c[0] ), Convert.ToSingle( c[1] ), Convert.ToSingle( c[2] ) );
					}
				}
				return arr;
			}
			return null;
		}

		private static Rotation[] GetQuatArray( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				var arr = new Rotation[list.Count];
				for ( int i = 0; i < arr.Length; i++ )
				{
					if ( list[i] is System.Collections.Generic.List<object> c && c.Count >= 4 )
					{
						arr[i] = new Rotation( Convert.ToSingle( c[0] ), Convert.ToSingle( c[1] ), Convert.ToSingle( c[2] ), Convert.ToSingle( c[3] ) );
					}
				}
				return arr;
			}
			return null;
		}

		private static float[] GetFloatArray( System.Collections.Generic.Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) && v is System.Collections.Generic.List<object> list )
			{
				var arr = new float[list.Count];
				for ( int i = 0; i < arr.Length; i++ ) arr[i] = Convert.ToSingle( list[i] );
				return arr;
			}
			return null;
		}

		private static string FourCC( uint v )
		{
			Span<char> c = stackalloc char[4];
			c[0] = (char)(v & 0xFF);
			c[1] = (char)((v >> 8) & 0xFF);
			c[2] = (char)((v >> 16) & 0xFF);
			c[3] = (char)((v >> 24) & 0xFF);
			return new string( c );
		}

		/// <summary>
		/// Decompress Valve's compressed normal format (based on ValveResourceFormat)
		/// </summary>
		private static Vector3 DecompressValveNormal(byte xByte, byte yByte)
		{
			float x = xByte;
			float y = yByte;
			
			x -= 128.0f;
			y -= 128.0f;
			
			var zSignBit = x < 0 ? 1.0f : 0.0f;    // z and t negative bits
			var tSignBit = y < 0 ? 1.0f : 0.0f;
			var zSign = -((2 * zSignBit) - 1);     // z and t signs
			var tSign = -((2 * tSignBit) - 1);
			
			x = (x * zSign) - zSignBit;            // 0..127
			y = (y * tSign) - tSignBit;
			x -= 64;                               // -64..63
			y -= 64;
			
			var xSignBit = x < 0 ? 1.0f : 0.0f;    // x and y negative bits
			var ySignBit = y < 0 ? 1.0f : 0.0f;
			var xSign = -((2 * xSignBit) - 1);     // x and y signs
			var ySign = -((2 * ySignBit) - 1);
			
			x = ((x * xSign) - xSignBit) / 63.0f;  // 0..1 range
			y = ((y * ySign) - ySignBit) / 63.0f;
			var z = 1.0f - x - y;
			
			var oolen = 1.0f / MathF.Sqrt((x * x) + (y * y) + (z * z)); // Normalize and
			x *= oolen * xSign;                   // Recover signs
			y *= oolen * ySign;
			z *= oolen * zSign;
			
			return new Vector3(x, y, z);
		}

		private static Model CreatePlaceholderTriangle()
		{
			var verts = new List<SimpleVertex>
			{
				new SimpleVertex( new Vector3(0,0,0), Vector3.Up, Vector3.Zero, Vector2.Zero ),
				new SimpleVertex( new Vector3(10,0,0), Vector3.Up, Vector3.Zero, new Vector2(1,0) ),
				new SimpleVertex( new Vector3(0,10,0), Vector3.Up, Vector3.Zero, new Vector2(0,1) ),
			};
			var indices = new []{ 0,1,2 };
			var mesh = new Mesh( Material.Create("model","simple_color",true) );
			mesh.Bounds = BBox.FromPoints( verts.Select(v=>v.position) );
			mesh.CreateVertexBuffer<SimpleVertex>( verts.Count, SimpleVertex.Layout, verts );
			mesh.CreateIndexBuffer( indices.Length, indices );
			return Model.Builder.WithName("hla_placeholder").AddMesh(mesh).Create();
		}

		private static void CreatePhysicsBodies( ModelBuilder builder, 
			(Dictionary<string,object> physKV, List<object> partsList)? physicsPartsData,
			(Dictionary<string,object> physKV, List<object> jointsList)? physicsJointsData )
		{
			if ( physicsPartsData == null && physicsJointsData == null ) return;

			var physicsBodies = new List<PhysicsBodyBuilder>();
			var bodyIdMap = new Dictionary<int, int>(); // Map part index to body builder index

			// Create physics bodies from parts data
			if ( physicsPartsData.HasValue )
			{
				var (physKV, partsList) = physicsPartsData.Value;
                if (EnableNonAnimLogs) Log.Info($"[HLA] Creating {partsList.Count} physics bodies");

				// Safety check: limit physics bodies to prevent crashes
				int maxBodies = Math.Min(partsList.Count, 100);
				if (partsList.Count > 100)
				{
					Log.Warning($"[HLA] Limiting physics bodies from {partsList.Count} to {maxBodies} for safety");
				}

				for ( int partIndex = 0; partIndex < maxBodies; partIndex++ )
				{
					if ( partsList[partIndex] is Dictionary<string,object> part )
					{
						// Debug: Log what's in this part
                        if (EnableNonAnimLogs) Log.Info($"[HLA] Part {partIndex} keys: [{string.Join(',', part.Keys)}]");
						
						// Create physics body for this part - with safety checks
						try
						{
							var physicsBody = builder.AddBody( 1.0f ); // Default mass
						physicsBodies.Add( physicsBody );
						bodyIdMap[partIndex] = physicsBodies.Count - 1;

						// Get bone name if available
						string boneName = null;
						if ( physKV.TryGetValue( "m_boneNames", out var boneNamesObj ) && boneNamesObj is List<object> boneNamesList )
						{
							if ( partIndex < boneNamesList.Count && boneNamesList[partIndex] is string name )
							{
								boneName = name;
								physicsBody.BoneName = boneName;
							}
						}

						// Parse collision shapes from part data
						bool addedCollision = false;
						
						// Look for shape data in the part - HLA uses m_rnShape
						if ( part.TryGetValue( "m_rnShape", out var shapeObj ) && shapeObj is Dictionary<string,object> shape )
						{
							addedCollision = ParsePhysicsShape( physicsBody, shape );
						}
						
						// Fallback: create a reasonable-sized box if no collision shapes found
						if ( !addedCollision )
						{
							var hullPoints = new Vector3[]
							{
								new Vector3(-5.0f, -5.0f, -5.0f),
								new Vector3( 5.0f, -5.0f, -5.0f),
								new Vector3( 5.0f,  5.0f, -5.0f),
								new Vector3(-5.0f,  5.0f, -5.0f),
								new Vector3(-5.0f, -5.0f,  5.0f),
								new Vector3( 5.0f, -5.0f,  5.0f),
								new Vector3( 5.0f,  5.0f,  5.0f),
								new Vector3(-5.0f,  5.0f,  5.0f)
							};
							physicsBody.AddHull( hullPoints );
                            if (EnableNonAnimLogs) Log.Info($"[HLA] Used fallback box collision for body {partIndex}");
						}

                        if (EnableNonAnimLogs) Log.Info($"[HLA] Created physics body {partIndex} (bone: {boneName ?? "none"})");
						}
						catch (Exception ex)
						{
							Log.Warning($"[HLA] Failed to create physics body {partIndex}: {ex.Message}");
						}
					}
				}
			}

			// Create joints between bodies
			if ( physicsJointsData.HasValue && physicsBodies.Count > 0 )
			{
				var (physKV, jointsList) = physicsJointsData.Value;
                if (EnableNonAnimLogs) Log.Info($"[HLA] Creating {jointsList.Count} physics joints");

				foreach ( var jointObj in jointsList )
				{
					if ( jointObj is Dictionary<string,object> joint )
					{
						var jointType = GetInt( joint, "m_nType" );
						var body1Index = GetInt( joint, "m_nBody1" );
						var body2Index = GetInt( joint, "m_nBody2" );

						// Parse joint frames (position + orientation as 7-float arrays: [x,y,z,qx,qy,qz,qw])
						var frame1Array = GetFloatArray( joint, "m_Frame1" );
						var frame2Array = GetFloatArray( joint, "m_Frame2" );

						if ( frame1Array?.Length >= 7 && frame2Array?.Length >= 7 && 
							 body1Index.HasValue && body2Index.HasValue &&
							 bodyIdMap.ContainsKey( body1Index.Value ) && bodyIdMap.ContainsKey( body2Index.Value ) )
						{
							var frame1 = new Transform(
								new Vector3(frame1Array[0], frame1Array[1], frame1Array[2]),
								new Rotation(frame1Array[3], frame1Array[4], frame1Array[5], frame1Array[6])
							);
							var frame2 = new Transform(
								new Vector3(frame2Array[0], frame2Array[1], frame2Array[2]),
								new Rotation(frame2Array[3], frame2Array[4], frame2Array[5], frame2Array[6])
							);

							// Parse swing and twist limits
							float swingMax = 30.0f; // Default
							float twistMin = -15.0f; // Default  
							float twistMax = 15.0f; // Default

							if ( joint.TryGetValue( "m_SwingLimit", out var swingLimitObj ) && swingLimitObj is Dictionary<string,object> swingLimit )
							{
								var swingMaxRad = GetFloat( swingLimit, "m_flMax" );
								if ( swingMaxRad.HasValue ) swingMax = swingMaxRad.Value * (180.0f / MathF.PI); // Convert to degrees
							}

							if ( joint.TryGetValue( "m_TwistLimit", out var twistLimitObj ) && twistLimitObj is Dictionary<string,object> twistLimit )
							{
								var twistMinRad = GetFloat( twistLimit, "m_flMin" );
								var twistMaxRad = GetFloat( twistLimit, "m_flMax" );
								if ( twistMinRad.HasValue ) twistMin = twistMinRad.Value * (180.0f / MathF.PI);
								if ( twistMaxRad.HasValue ) twistMax = twistMaxRad.Value * (180.0f / MathF.PI);
							}

							// Create joint between the two bodies
							var body1Id = bodyIdMap[body1Index.Value];
							var body2Id = bodyIdMap[body2Index.Value];

							var jointBuilder = builder.AddBallJoint( body1Id, body2Id, frame1, frame2, false );

							// Apply limits based on joint type
							if ( jointType == 4 ) // Ball joint
							{
								jointBuilder.WithSwingLimit( swingMax ).WithTwistLimit( twistMin, twistMax );
							}
							else if ( jointType == 3 ) // Hinge joint  
							{
								// Hinge joints only have twist limits (rotation around single axis)
								jointBuilder.WithTwistLimit( twistMin, twistMax );
							}

                            if (EnableNonAnimLogs) Log.Info($"[HLA] Created joint type={jointType} body1={body1Index} body2={body2Index} swing={swingMax:F1}° twist=[{twistMin:F1}°,{twistMax:F1}°]");
						}
					}
				}
			}
		}

		private static bool ParsePhysicsShape( PhysicsBodyBuilder physicsBody, Dictionary<string,object> shape )
		{
			bool addedAnyShape = false;

			// Parse spheres - HLA structure: m_spheres[i].m_Sphere.m_vCenter + m_flRadius
			if ( shape.TryGetValue( "m_spheres", out var spheresObj ) && spheresObj is List<object> spheresList )
			{
				foreach ( var sphereObj in spheresList )
				{
					if ( sphereObj is Dictionary<string,object> sphereData &&
						 sphereData.TryGetValue( "m_Sphere", out var sphereGeomObj ) && sphereGeomObj is Dictionary<string,object> sphereGeom )
					{
						var centerArray = GetFloatArray( sphereGeom, "m_vCenter" );
						var radius = GetFloat( sphereGeom, "m_flRadius" );
						if ( centerArray?.Length >= 3 && radius.HasValue )
						{
							var center = new Vector3( centerArray[0], centerArray[1], centerArray[2] );
							var radiusVal = radius.Value;
							
							// Create a spherical hull approximation (octahedral)
							var hullPoints = new List<Vector3>();
							
							hullPoints.Add( center + Vector3.Up * radiusVal );
							hullPoints.Add( center + Vector3.Down * radiusVal );
							hullPoints.Add( center + Vector3.Left * radiusVal );
							hullPoints.Add( center + Vector3.Right * radiusVal );
							hullPoints.Add( center + Vector3.Forward * radiusVal );
							hullPoints.Add( center + Vector3.Backward * radiusVal );
							
							physicsBody.AddHull( hullPoints.ToArray() );
							addedAnyShape = true;
                            if (EnableNonAnimLogs) Log.Info($"[HLA] Added sphere collision as hull: center={center} radius={radius}");
						}
					}
				}
			}

			// Parse capsules - HLA structure: m_capsules[i].m_Capsule.m_vCenter (array of 2 points) + m_flRadius
			if ( shape.TryGetValue( "m_capsules", out var capsulesObj ) && capsulesObj is List<object> capsulesList )
			{
				foreach ( var capsuleObj in capsulesList )
				{
					if ( capsuleObj is Dictionary<string,object> capsuleData &&
						 capsuleData.TryGetValue( "m_Capsule", out var capsuleGeomObj ) && capsuleGeomObj is Dictionary<string,object> capsuleGeom )
					{
						var radius = GetFloat( capsuleGeom, "m_flRadius" );
						if ( capsuleGeom.TryGetValue( "m_vCenter", out var centerArrayObj ) && centerArrayObj is List<object> centerPoints && 
							 centerPoints.Count >= 2 && radius.HasValue )
						{
							// Parse the two center points
							Vector3? point1 = null, point2 = null;
							
							if ( centerPoints[0] is List<object> p1Coords && p1Coords.Count >= 3 )
							{
								point1 = new Vector3(
									Convert.ToSingle( p1Coords[0] ), 
									Convert.ToSingle( p1Coords[1] ), 
									Convert.ToSingle( p1Coords[2] )
								);
							}
							
							if ( centerPoints[1] is List<object> p2Coords && p2Coords.Count >= 3 )
							{
								point2 = new Vector3(
									Convert.ToSingle( p2Coords[0] ), 
									Convert.ToSingle( p2Coords[1] ), 
									Convert.ToSingle( p2Coords[2] )
								);
							}
							
							if ( point1.HasValue && point2.HasValue )
							{
								// Convert capsule to hull (cylinder-like shape) since s&box AddCapsule API is different
								var center = (point1.Value + point2.Value) * 0.5f;
								var height = Vector3.DistanceBetween( point1.Value, point2.Value );
								var direction = (point2.Value - point1.Value).Normal;
								var radiusVal = radius.Value;
								
								// Create a cylindrical hull to approximate the capsule
								var hullPoints = new List<Vector3>();
								var segments = 8; // 8-sided cylinder
								
								for ( int i = 0; i < segments; i++ )
								{
									var angle = (float)(i * 2.0 * Math.PI / segments);
									var perpendicular = Vector3.Cross( direction, Vector3.Up ).Normal;
									if ( perpendicular.Length < 0.1f )
										perpendicular = Vector3.Cross( direction, Vector3.Forward ).Normal;
									var perpendicular2 = Vector3.Cross( direction, perpendicular ).Normal;
									
									var offset = perpendicular * MathF.Cos( angle ) * radiusVal + perpendicular2 * MathF.Sin( angle ) * radiusVal;
									
									// Add points at both ends of the capsule
									hullPoints.Add( point1.Value + offset );
									hullPoints.Add( point2.Value + offset );
								}
								
								physicsBody.AddHull( hullPoints.ToArray() );
								addedAnyShape = true;
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Added capsule collision as hull: {point1} to {point2} radius={radius} height={height:F2}");
							}
						}
					}
				}
			}

			// Hulls (convex shapes)
			if ( shape.TryGetValue( "m_hulls", out var hullsObj ) && hullsObj is List<object> hullsList )
			{
				foreach ( var hullObj in hullsList )
				{
					if ( hullObj is Dictionary<string,object> hull )
					{
						// Parse hull vertices
						if ( hull.TryGetValue( "m_vertices", out var verticesObj ) && verticesObj is List<object> verticesList )
						{
							var hullPoints = new List<Vector3>();
							foreach ( var vertexObj in verticesList )
							{
								if ( vertexObj is List<object> vertexCoords && vertexCoords.Count >= 3 )
								{
									var x = Convert.ToSingle( vertexCoords[0] );
									var y = Convert.ToSingle( vertexCoords[1] );
									var z = Convert.ToSingle( vertexCoords[2] );
									hullPoints.Add( new Vector3( x, y, z ) );
								}
							}
							
							if ( hullPoints.Count >= 4 ) // Need at least 4 points for a valid hull
							{
								physicsBody.AddHull( hullPoints.ToArray() );
								addedAnyShape = true;
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Added hull collision with {hullPoints.Count} vertices");
							}
						}
					}
				}
			}

			// Meshes (triangle meshes for static collision)
			if ( shape.TryGetValue( "m_meshes", out var meshesObj ) && meshesObj is List<object> meshesList )
			{
				foreach ( var meshObj in meshesList )
				{
					if ( meshObj is Dictionary<string,object> mesh )
					{
						// Parse mesh vertices and triangles
						if ( mesh.TryGetValue( "m_vertices", out var meshVerticesObj ) && meshVerticesObj is List<object> meshVerticesList &&
							 mesh.TryGetValue( "m_triangles", out var trianglesObj ) && trianglesObj is List<object> trianglesList )
						{
							var meshPoints = new List<Vector3>();
							var meshIndices = new List<int>();

							// Parse vertices
							foreach ( var vertexObj in meshVerticesList )
							{
								if ( vertexObj is List<object> vertexCoords && vertexCoords.Count >= 3 )
								{
									var x = Convert.ToSingle( vertexCoords[0] );
									var y = Convert.ToSingle( vertexCoords[1] );
									var z = Convert.ToSingle( vertexCoords[2] );
									meshPoints.Add( new Vector3( x, y, z ) );
								}
							}

							// Parse triangle indices
							foreach ( var triangleObj in trianglesList )
							{
								if ( triangleObj is List<object> triangleIndices && triangleIndices.Count >= 3 )
								{
									meshIndices.Add( Convert.ToInt32( triangleIndices[0] ) );
									meshIndices.Add( Convert.ToInt32( triangleIndices[1] ) );
									meshIndices.Add( Convert.ToInt32( triangleIndices[2] ) );
								}
							}

							if ( meshPoints.Count > 0 && meshIndices.Count > 0 )
							{
								// For now, create a hull from the mesh vertices (simplified)
								physicsBody.AddHull( meshPoints.ToArray() );
								addedAnyShape = true;
                                if (EnableNonAnimLogs) Log.Info($"[HLA] Added mesh collision with {meshPoints.Count} vertices, {meshIndices.Count/3} triangles (as hull)");
							}
						}
					}
				}
			}

			return addedAnyShape;
		}

		private static List<string> FindAllMeshMaterials( Stream s, List<(uint offset, uint size, string name)> blockTable )
		{
			var materials = new List<string>();
			
			// Look in DATA and MDAT blocks for material references
			foreach ( var (offset, size, name) in blockTable )
			{
				if ( name != "DATA" && name != "MDAT" ) continue;
				
				try
				{
					s.Seek( offset, SeekOrigin.Begin );
					var blockData = new byte[size];
					s.Read( blockData );
					
					var kv = BinaryKV3Lite.Parse( blockData );
					if ( kv == null ) continue;
					
					// Look for scene objects with draw calls
					if ( kv.TryGetValue( "m_sceneObjects", out var sceneObj ) && sceneObj is List<object> sceneList )
					{
						foreach ( var sceneItem in sceneList )
						{
							if ( sceneItem is Dictionary<string,object> scene )
							{
								// Look for draw calls with material references
								if ( scene.TryGetValue( "m_drawCalls", out var drawCallsObj ) && drawCallsObj is List<object> drawCallsList )
								{
									foreach ( var drawCallItem in drawCallsList )
									{
										if ( drawCallItem is Dictionary<string,object> drawCall )
										{
											var material = GetString( drawCall, "m_material" ) ?? GetString( drawCall, "m_pMaterial" );
											if ( !string.IsNullOrEmpty( material ) && !materials.Contains( material ) )
											{
												materials.Add( material );
                    if (EnableNonAnimLogs) Log.Info($"[HLA] Found material reference: {material}");
											}
										}
									}
								}
							}
						}
					}
				}
				catch ( Exception ex )
				{
					Log.Warning($"[HLA] Failed to parse {name} for materials: {ex.Message}");
				}
			}
			
			return materials;
		}

		private static void AddMaterialGroups( ModelBuilder builder, Stream s, List<(uint offset, uint size, string name)> blockTable, HlaMount hlaMount )
		{
			Log.Info($"[HLA] AddMaterialGroups: Checking {blockTable.Count} blocks");
			
			// Look in DATA and MDAT blocks for material groups
			foreach ( var (offset, size, name) in blockTable )
			{
				Log.Info($"[HLA] Checking block: {name} at offset {offset}, size {size}");
				
				if ( name != "DATA" && name != "MDAT" ) continue;
				
				try
				{
					s.Seek( offset, SeekOrigin.Begin );
					var blockData = new byte[size];
					s.Read( blockData );
					
					var kv = BinaryKV3Lite.Parse( blockData );
					if ( kv == null ) 
					{
						Log.Info($"[HLA] Failed to parse KV3 from {name} block");
						continue;
					}
					
					Log.Info($"[HLA] Successfully parsed KV3 from {name} block, keys: {string.Join(", ", kv.Keys)}");
					
					// Look for material groups
					if ( kv.TryGetValue( "m_materialGroups", out var materialGroupsObj ) && materialGroupsObj is List<object> materialGroupsList )
					{
                        Log.Info($"[HLA] Found {materialGroupsList.Count} material groups");
						
						foreach ( var groupObj in materialGroupsList )
						{
							if ( groupObj is Dictionary<string,object> group )
							{
								var groupName = GetString( group, "m_name" );
								if ( string.IsNullOrEmpty( groupName ) ) continue;
								
								if ( group.TryGetValue( "m_materials", out var materialsObj ) && materialsObj is List<object> materialsList )
								{
									var groupBuilder = builder.AddMaterialGroup( groupName );
                                    if (EnableNonAnimLogs) Log.Info($"[HLA] Material group '{groupName}' has {materialsList.Count} materials");
									
									foreach ( var materialObj in materialsList )
									{
										if ( materialObj is string materialPath )
										{
											// Remove "resource:" prefix if present
											var cleanPath = materialPath.StartsWith( "resource:" ) ? materialPath.Substring( 9 ) : materialPath;
											var material = LoadHLAMaterial( cleanPath, hlaMount );
											groupBuilder.AddMaterial( material );
                                            if (EnableNonAnimLogs) Log.Info($"[HLA] Added material to group '{groupName}': {cleanPath}");
										}
									}
								}
							}
						}
						
						return; // Found material groups, done
					}
					else
					{
						Log.Info($"[HLA] No m_materialGroups found in {name} block");
					}
				}
				catch ( Exception ex )
				{
					Log.Warning($"[HLA] Failed to parse {name} for material groups: {ex.Message}");
				}
			}
			
			Log.Info($"[HLA] No material groups found in any DATA/MDAT blocks");
		}

		private static Material LoadHLAMaterial( string materialPath, HlaMount hlaMount )
		{
			if ( string.IsNullOrEmpty( materialPath ) )
			{
                Log.Info($"[HLA] No material path provided, using default");
				var defaultMat = Material.Create( "model", "simple_color" );
				defaultMat?.Set( "Color", Texture.White );
				return defaultMat;
			}
			
			try
			{
				// Convert to proper VPK path for HLA material loader
				var vpkPath = materialPath;
				if ( !vpkPath.EndsWith( ".vmat_c" ) )
				{
					vpkPath += "_c";
				}
				
                Log.Info($"[HLA] Loading HLA material: {materialPath} -> {vpkPath}");
				
				// Use our HLA material loader instead of s&box's default loader
				if ( hlaMount != null )
				{
					var materialLoader = new HLAMaterialLoader( hlaMount, vpkPath );
					var material = materialLoader.LoadImmediate();
					
					if ( material != null )
					{
						Log.Info($"[HLA] Successfully loaded HLA material: {materialPath}");
						return material;
					}
					else
					{
						Log.Warning($"[HLA] HLA material loader returned null for: {materialPath}");
					}
				}
				else
				{
					Log.Warning($"[HLA] No HLA mount provided for material loading");
				}
			}
			catch ( Exception ex )
			{
				Log.Warning($"[HLA] Failed to load HLA material {materialPath}: {ex.Message}");
			}
			
			// Fallback to default material
            Log.Info($"[HLA] Using fallback material for: {materialPath}");
			var fallbackMat = Material.Create( "model", "simple_color" );
			fallbackMat?.Set( "Color", Texture.White );
			return fallbackMat;
		}

		private int TryAddAnimationsFromEmbeddedBlocks( ModelBuilder builder, List<(uint type, uint offset, uint size)> blocks, Stream s, BinaryReader br, int skeletonBoneCount, Dictionary<string,int> boneNameToIndex, Vector3[] bindLocalPos, Rotation[] bindLocalRot, Dictionary<string,object> modelDecodeKeyKV, int[] skeletonParents )
		{
			int added = 0;
			
			// Safety check: limit processing to prevent crashes
			if (blocks == null || blocks.Count > 1000)
			{
				Log.Warning($"[HLA] Too many blocks ({blocks?.Count}), skipping animation processing");
				return 0;
			}

			// First pass: Process AGRP blocks to cache data channels
			foreach ( var blk in blocks )
			{
				var name = FourCC( blk.type );
				if ( name == "AGRP" && blk.size > 0 )
				{
					try
					{
						//Log.Info($"[HLA] Pre-processing AGRP block @0x{blk.offset:X} size={blk.size}");
						s.Position = blk.offset;
						var blob = br.ReadBytes( (int)blk.size );
						var kv = BinaryKV3Lite.Parse( blob );
						if ( kv != null )
						{
							//Log.Info($"[HLA] AGRP keys: {string.Join(", ", kv.Keys)}");
							if ( kv.TryGetValue("m_dataChannelArray", out var dcaObj) )
							{
								cachedDataChannelArray = kv; // Cache the entire AGRP structure
								//Log.Info($"[HLA] Cached data channels from AGRP block");
							}
							else if ( kv.TryGetValue("m_decodeKey", out var decodeKeyObj) && decodeKeyObj is Dictionary<string,object> decodeKey )
							{
								//Log.Info($"[HLA] AGRP decodeKey keys: {string.Join(", ", decodeKey.Keys)}");
								if ( decodeKey.TryGetValue("m_dataChannelArray", out var dcaObj2) )
								{
									cachedDataChannelArray = decodeKey; // Cache the decodeKey structure
									//Log.Info($"[HLA] Cached data channels from AGRP decodeKey");
								}
							}
						}
					}
					catch ( Exception ex )
					{
						Log.Info($"[HLA] Error pre-processing AGRP block: {ex.Message}");
					}
				}
			}
			
			// Second pass: Process all blocks
			foreach ( var blk in blocks )
			{
				var name = FourCC( blk.type );
				if ( name != "ANIM" && name != "ASEQ" && name != "AGRP" ) continue;
				if ( blk.size == 0 ) continue;
				try
				{
					Log.Info($"[HLA] Parsing embedded {name} block @0x{blk.offset:X} size={blk.size}");
					s.Position = blk.offset;
					var blob = br.ReadBytes( (int)blk.size );
					var kv = BinaryKV3Lite.Parse( blob );
					if ( kv == null )
					{
						// Try legacy seg-decoded format inside ANIM's DATA-like KV: seek for m_animArray
						var txt = Encoding.UTF8.GetString( blob );
						if ( name == "ANIM" && txt.Contains("m_animArray", StringComparison.Ordinal) )
						{
							var legacyAdded = TryParseLegacyAnimKVText( builder, txt, boneNameToIndex, skeletonBoneCount );
							added += legacyAdded;
						}
						else
						{
							Log.Info($"[HLA] {name} block is not KV3 - skipping");
						}
						continue;
					}

					// Check for AGRP data channels first
					if ( name == "AGRP" && kv.TryGetValue("m_dataChannelArray", out var dcaObj) )
					{
						cachedDataChannelArray = kv; // Cache the entire AGRP structure
						Log.Info($"[HLA] Cached data channels from AGRP block");
					}

					var clips = new List<Dictionary<string,object>>();
					CollectClipCandidatesRecursive( kv, clips );
                    if ( clips.Count == 0 )
                    {
                        // Try legacy segmented format (decoder/segment arrays)
                        var legacyAdded = 0;
                        if ( name == "ANIM" )
                        {
							legacyAdded = TryDecodeLegacyAnimKV( builder, kv, modelDecodeKeyKV, boneNameToIndex, skeletonBoneCount, bindLocalPos, bindLocalRot, skeletonParents );
                        }
                        if ( legacyAdded > 0 )
                        {
                            Log.Info($"[HLA] Added {legacyAdded} legacy anim(s) from {name}");
                            added += legacyAdded;
                            continue;
                        }
                        Log.Info($"[HLA] {name} block contains no clip candidates");
                        continue;
                    }

					int idx = 0;
					foreach ( var clip in clips )
					{
						try
						{
							var clipName = GetString( clip, "m_name" ) ?? $"clip_{name}_{idx++}";
							if ( AddClipFromKV( builder, clipName, clip, skeletonBoneCount ) ) added++;
						}
						catch ( Exception ex )
						{
							Log.Warning($"[HLA] Failed to add embedded clip: {ex.Message}");
						}
					}
				}
				catch ( Exception ex )
				{
					Log.Warning($"[HLA] Embedded animation parse failed for {name}: {ex.Message}");
				}
			}
			if ( added > 0 ) Log.Info($"[HLA] Added {added} embedded animation clip(s)");
			return added;
		}

		private static void CollectClipCandidatesRecursive( object node, List<Dictionary<string,object>> output )
		{
			if ( node == null ) return;
			if ( node is Dictionary<string,object> d )
			{
				// Heuristic: a clip has compression settings + pose data
				bool hasSettings = d.TryGetValue( "m_trackCompressionSettings", out var tcsObj ) && tcsObj is List<object>;
				bool hasData = d.ContainsKey( "m_compressedPoseData" ) && d.ContainsKey( "m_compressedPoseOffsets" );
				bool hasFrames = d.ContainsKey( "m_nNumFrames" );
				if ( hasSettings && hasData && hasFrames )
				{
					output.Add( d );
				}
				foreach ( var v in d.Values ) CollectClipCandidatesRecursive( v, output );
				return;
			}
			if ( node is List<object> list )
			{
				foreach ( var v in list ) CollectClipCandidatesRecursive( v, output );
			}
		}

		private bool AddClipFromKV( ModelBuilder builder, string animName, Dictionary<string,object> kv, int skeletonBoneCount )
		{
			int numFrames = GetInt( kv, "m_nNumFrames" ) ?? 0;
			float duration = GetFloat( kv, "m_flDuration" ) ?? 0.0f;
			if ( numFrames <= 0 || duration <= 0 ) return false;
			float fps = numFrames / MathF.Max( duration, 1e-6f );

			var settings = new List<TrackSetting>();
			if ( kv.TryGetValue( "m_trackCompressionSettings", out var tcsObj ) && tcsObj is List<object> tcs )
			{
				foreach ( var entry in tcs )
				{
					if ( entry is not Dictionary<string,object> d ) continue;
					var tx = GetRange( d, "m_translationRangeX" );
					var ty = GetRange( d, "m_translationRangeY" );
					var tz = GetRange( d, "m_translationRangeZ" );
					var sc = GetRange( d, "m_scaleRange" );
					var constRotArr = GetFloatArray( d, "m_constantRotation" );
					var constRot = (constRotArr != null && constRotArr.Length >= 4) ? new Rotation( constRotArr[0], constRotArr[1], constRotArr[2], constRotArr[3] ) : Rotation.Identity;
					bool isRotStatic = GetBool( d, "m_bIsRotationStatic" );
					bool isTrStatic = GetBool( d, "m_bIsTranslationStatic" );
					bool isScStatic = GetBool( d, "m_bIsScaleStatic" );
					settings.Add( new TrackSetting { TRX = tx, TRY = ty, TRZ = tz, Scale = sc, ConstantRotation = constRot, IsRotationStatic = isRotStatic, IsTranslationStatic = isTrStatic, IsScaleStatic = isScStatic } );
				}
			}

			byte[] compressedData = null;
			if ( kv.TryGetValue( "m_compressedPoseData", out var cpdObj ) )
			{
				if ( cpdObj is byte[] bArr ) compressedData = bArr;
				else if ( cpdObj is List<object> list )
				{
					compressedData = new byte[list.Count];
					for ( int i = 0; i < list.Count; i++ ) compressedData[i] = Convert.ToByte( list[i] );
				}
			}
			if ( compressedData == null ) return false;

			int[] offsets = null;
			if ( kv.TryGetValue( "m_compressedPoseOffsets", out var offObj ) )
			{
				if ( offObj is List<object> l )
				{
					offsets = new int[l.Count];
					for ( int i = 0; i < l.Count; i++ ) offsets[i] = Convert.ToInt32( l[i] );
				}
			}
			if ( offsets == null || offsets.Length != numFrames ) Log.Warning("[HLA] embedded anim offsets missing or mismatched");

			var animationBuilder = builder.AddAnimation( animName, (int)MathF.Round( fps ) );
			var dataU16 = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ushort>( compressedData );

			int trackCount = settings.Count;
			int boneCount = Math.Min( skeletonBoneCount, trackCount );
			for ( int frame = 0; frame < numFrames; frame++ )
			{
				var transforms = new Transform[skeletonBoneCount];
				for ( int i = 0; i < skeletonBoneCount; i++ ) transforms[i] = new Transform( Vector3.Zero, Rotation.Identity );

				int baseIndex = (offsets != null && frame < offsets.Length) ? offsets[frame] : 0;
				int di = baseIndex;

				for ( int i = 0; i < boneCount; i++ )
				{
					var cfg = settings[i];
					var pos = new Vector3( cfg.TRX.Start, cfg.TRY.Start, cfg.TRZ.Start );
					var rot = cfg.ConstantRotation;
					float scl = cfg.Scale.Start;

					if ( !cfg.IsRotationStatic ) rot = DecodeQuat15( dataU16, ref di );
					if ( !cfg.IsTranslationStatic )
					{
						pos = new Vector3(
							DecodeUnormToRange( dataU16[di++], cfg.TRX.Start, cfg.TRX.Length ),
							DecodeUnormToRange( dataU16[di++], cfg.TRY.Start, cfg.TRY.Length ),
							DecodeUnormToRange( dataU16[di++], cfg.TRZ.Start, cfg.TRZ.Length )
						);
					}
					if ( !cfg.IsScaleStatic ) scl = DecodeUnormToRange( dataU16[di++], cfg.Scale.Start, cfg.Scale.Length );

					transforms[i] = new Transform( pos, rot );
				}

				animationBuilder.AddFrame( transforms );
			}
			return true;
		}

		// --- Animation import (HL:A .vanim_c) ---
		private void AddAnimationsForModel( ModelBuilder builder, string vpkPath, int skeletonBoneCount )
		{
			try
			{
				var refs = new HashSet<string>( StringComparer.OrdinalIgnoreCase );
				CollectAnimationRefsFromResource( vpkPath, refs, 2 );
				if ( refs.Count == 0 )
				{
					Log.Info("[HLA] No animation references found in model");
					return;
				}

				Log.Info($"[HLA] Animation references: {string.Join(", ", refs)}");
				foreach ( var animPath in refs )
				{
					if ( animPath.EndsWith( ".vanim", StringComparison.OrdinalIgnoreCase ) || animPath.EndsWith( ".vanim_c", StringComparison.OrdinalIgnoreCase ) )
					{
						try { LoadVanimAndAddToModel( builder, animPath, skeletonBoneCount ); }
						catch ( Exception ex ) { Log.Warning($"[HLA] Failed to load anim '{animPath}': {ex.Message}"); }
					}
				}
			}
			catch ( Exception ex )
			{
				Log.Warning($"[HLA] AddAnimationsForModel error: {ex.Message}");
			}
		}

		private void CollectAnimationRefsFromResource( string compiledPath, HashSet<string> output, int depth )
		{
			if ( depth <= 0 ) return;
			byte[] bytes;
			try { bytes = base.Host.GetFileBytes( compiledPath ); }
			catch { return; }

			using var ms = new MemoryStream( bytes );
			using var br = new BinaryReader( ms );
			uint _fileSize = br.ReadUInt32();
			ushort _headerVer = br.ReadUInt16();
			ushort _typeVer = br.ReadUInt16();
			uint blockOffset = br.ReadUInt32();
			uint blockCount = br.ReadUInt32();

			var blocks = new List<(string name, uint offset, uint size)>();
			ms.Position += blockOffset - 8;
			for ( int i = 0; i < blockCount; i++ )
			{
				uint blockType = br.ReadUInt32();
				long pos = ms.Position;
				uint rel = br.ReadUInt32();
				uint size = br.ReadUInt32();
				blocks.Add( (FourCC(blockType), (uint)(pos + rel), size) );
				ms.Position = pos + 8;
			}

			foreach ( var (name, offset, size) in blocks )
			{
				if ( name != "DATA" && name != "MDAT" ) continue;
				if ( size == 0 ) continue;
				try
				{
					ms.Position = offset;
					var blob = br.ReadBytes( (int)size );
					var kv = BinaryKV3Lite.Parse( blob );
					if ( kv == null ) continue;
					CollectAnimStringsRecursive( kv, output );
				}
				catch { }
			}

			// Also scan RERL (external references) or the whole file for plain-text paths
			try
			{
				ms.Position = 0;
				var allBytes = br.ReadBytes( (int)ms.Length );
				foreach ( var p in ExtractAnimPathsFromBytes( allBytes ) ) output.Add( p );
			}
			catch { }

			// Expand sequences one level to pull referenced clips
			var seqs = output.Where( s => s.EndsWith(".vseq", StringComparison.OrdinalIgnoreCase) || s.EndsWith(".vseq_c", StringComparison.OrdinalIgnoreCase) ).ToArray();
			foreach ( var seq in seqs )
			{
				CollectAnimationRefsFromResource( NormalizeCompiledPath(seq), output, depth - 1 );
			}
		}

		private static IEnumerable<string> ExtractAnimPathsFromBytes( byte[] data )
		{
			// Convert to a printable string (non-printable -> space) then split
			var sb = new System.Text.StringBuilder( data.Length );
			for ( int i = 0; i < data.Length; i++ )
			{
				byte b = data[i];
				if ( b >= 32 && b <= 126 ) sb.Append( (char)b );
				else sb.Append( ' ' );
			}
			var text = sb.ToString();
			var tokens = text.Split( new[]{ ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries );
			foreach ( var raw in tokens )
			{
				var t = raw.Trim().Trim('"','\'',';',',','>','<','(',')');
				if ( t.Length < 6 ) continue;
				var lower = t.ToLowerInvariant();
				if ( lower.Contains(".vanim") || lower.Contains(".vseq") )
				{
					// Normalize path
					var p = lower;
					if ( p.StartsWith("resource:" ) ) p = p.Substring(9);
					p = p.Replace('\\','/');
					yield return p;
				}
			}
		}

		// Decode legacy segmented animations stored in ANIM with m_animArray/m_decoderArray/m_segmentArray
		private static int TryDecodeLegacyAnimKV( ModelBuilder builder, Dictionary<string,object> animKV, Dictionary<string,object> decodeKeyKV, Dictionary<string,int> boneNameToIndex, int skeletonBoneCount, Vector3[] bindLocalPos, Rotation[] bindLocalRot, int[] skeletonParents )
		{
			if ( animKV == null ) return 0;
			if ( !animKV.TryGetValue( "m_animArray", out var animArrayObj ) || animArrayObj is not List<object> animList ) return 0;
			if ( !animKV.TryGetValue( "m_decoderArray", out var decodersObj ) || decodersObj is not List<object> decoderList ) return 0;
			if ( !animKV.TryGetValue( "m_segmentArray", out var segmentsObj ) || segmentsObj is not List<object> segmentsList ) return 0;

			// Build decoder name table
			var decoderNames = new List<string>( decoderList.Count );
			foreach ( var d in decoderList )
			{
				if ( d is Dictionary<string,object> dd ) decoderNames.Add( GetString( dd, "m_szName" ) ?? string.Empty );
			}

			// Prepare channels from decodeKey OR derive from segments (some ANIM blocks don't have explicit data channels)
			var channelAttrs = new List<string>();
			var channelRemaps = new List<int[]>(); // per channel: remapTable[boneIndex] = elementIndex or -1
			
			// Try to get data channels from decodeKey first
			bool hasDataChannels = false;
			//Log.Info($"[HLA] Starting animation decode with {skeletonBoneCount} bones");
			
			// First try cached AGRP data channels
			List<object> dcaList = null;
			if ( cachedDataChannelArray != null && cachedDataChannelArray.TryGetValue( "m_dataChannelArray", out var cachedDcaObj ) && cachedDcaObj is List<object> cachedDcaList )
			{
				dcaList = cachedDcaList;
				//Log.Info($"[HLA] Using cached AGRP data channels ({dcaList.Count} channels)");
			}
			// Fallback to decodeKeyKV data channels
			else if ( decodeKeyKV != null && decodeKeyKV.TryGetValue( "m_dataChannelArray", out var dcaObj ) && dcaObj is List<object> localDcaList )
			{
				dcaList = localDcaList;
				//Log.Info($"[HLA] Using local decodeKey data channels ({dcaList.Count} channels)");
			}
			
			if ( dcaList != null )
			{
				foreach ( var chObj in dcaList )
				{
					if ( chObj is not Dictionary<string,object> ch ) { channelAttrs.Add(string.Empty); channelRemaps.Add(new int[skeletonBoneCount]); continue; }
					var attr = (GetString( ch, "m_szVariableName" ) ?? string.Empty).ToLowerInvariant();
					int[] remap = new int[skeletonBoneCount];
					for ( int i = 0; i < remap.Length; i++ ) remap[i] = -1;
					List<object> elemNames = null; List<object> elemIdx = null;
					if ( ch.TryGetValue( "m_szElementNameArray", out var enaObj ) ) elemNames = enaObj as List<object>;
					if ( ch.TryGetValue( "m_nElementIndexArray", out var eiaObj ) ) elemIdx = eiaObj as List<object>;
					if ( elemNames != null && elemIdx != null )
					{
						int n = Math.Min( elemNames.Count, elemIdx.Count );
						for ( int i = 0; i < n; i++ )
						{
							var nm = elemNames[i] as string;
							if ( string.IsNullOrEmpty(nm) ) continue;
							if ( boneNameToIndex != null && boneNameToIndex.TryGetValue( nm, out var boneIdx ) )
							{
								remap[boneIdx] = Convert.ToInt32( elemIdx[i] );
							}
						}
					}
					channelAttrs.Add( attr );
					channelRemaps.Add( remap );
					int setCount = 0;
					for ( int j = 0; j < remap.Length; j++ ) if ( remap[j] != -1 ) setCount++;
					//Log.Info($"[HLA] dataChannel[{channelAttrs.Count-1}] attr='{attr}' mapped={setCount}/{remap.Length} bones");
					hasDataChannels = true;
				}
			}
			
			// If no data channels found, try to build mapping from bit arrays in animKV
			if ( !hasDataChannels )
			{
				//Log.Info($"[HLA] No explicit data channels found, trying to build mapping from bit arrays");
				
				// Try to get bit arrays from animKV structure
				bool foundBitArrays = false;
				if ( animKV.TryGetValue("m_pData", out var pDataObj) && pDataObj is Dictionary<string,object> pData )
				{
					if ( pData.TryGetValue("m_usageDifferences", out var usageObj) && usageObj is Dictionary<string,object> usage )
					{
						// Get movement (position) and rotation bit arrays
						List<object> movementBits = null;
						List<object> rotationBits = null;
						if ( usage.TryGetValue("m_bHasMovementBitArray", out var movObj) && movObj is List<object> movList ) movementBits = movList;
						if ( usage.TryGetValue("m_bHasRotationBitArray", out var rotObj) && rotObj is List<object> rotList ) rotationBits = rotList;
						
						if ( movementBits != null && rotationBits != null )
						{
							foundBitArrays = true;
							
							// Parse bit arrays to determine which bones have animation data
							var positionBones = ParseBitArray(movementBits);
							var angleBones = ParseBitArray(rotationBits);
							
							//Log.Info($"[HLA] Found bit arrays: {positionBones.Count} position bones, {angleBones.Count} angle bones");
							
							// Create Position channel (channel 0)
							channelAttrs.Add("position");
							int[] posRemap = new int[skeletonBoneCount];
							for ( int i = 0; i < posRemap.Length; i++ ) posRemap[i] = -1;
							
							// Map: bone index -> element index (sequential for bones that have data)
							int posElementIndex = 0;
							for ( int boneIdx = 0; boneIdx < skeletonBoneCount && boneIdx < positionBones.Count; boneIdx++ )
							{
								if ( positionBones[boneIdx] )
								{
									posRemap[boneIdx] = posElementIndex++;
								}
							}
							channelRemaps.Add(posRemap);
							
							// Create Angle channel (channel 1)
							channelAttrs.Add("angle");
							int[] angleRemap = new int[skeletonBoneCount];
							for ( int i = 0; i < angleRemap.Length; i++ ) angleRemap[i] = -1;
							
							// Map: bone index -> element index (sequential for bones that have data, offset by position count)
							int angleElementIndex = posElementIndex; // Start after position elements
							for ( int boneIdx = 0; boneIdx < skeletonBoneCount && boneIdx < angleBones.Count; boneIdx++ )
							{
								if ( angleBones[boneIdx] )
								{
									angleRemap[boneIdx] = angleElementIndex++;
								}
							}
							channelRemaps.Add(angleRemap);
							
							//Log.Info($"[HLA] Created channels from bit arrays: {posElementIndex} position elements, {angleElementIndex - posElementIndex} angle elements");
							//Log.Info($"[HLA] Position remap: bone0->elem{(posRemap.Length > 0 ? posRemap[0] : -1)}, bone1->elem{(posRemap.Length > 1 ? posRemap[1] : -1)}, bone2->elem{(posRemap.Length > 2 ? posRemap[2] : -1)}");
							//Log.Info($"[HLA] Angle remap: bone0->elem{(angleRemap.Length > 0 ? angleRemap[0] : -1)}, bone1->elem{(angleRemap.Length > 1 ? angleRemap[1] : -1)}, bone2->elem{(angleRemap.Length > 2 ? angleRemap[2] : -1)}");
						}
					}
				}
				
				// Fallback to segment analysis if bit arrays not found
				if ( !foundBitArrays )
				{
					//Log.Info($"[HLA] No bit arrays found, falling back to segment analysis");
					
					// Collect all unique element IDs from segments for each attribute type
				var positionElements = new HashSet<short>();
				var angleElements = new HashSet<short>();
				
				for ( int i = 0; i < segmentsList.Count; i++ )
				{
					if ( segmentsList[i] is not Dictionary<string,object> seg ) continue;
					
					// Get decoder to determine attribute type
					int decoderIndex = -1;
					byte[] container = null;
					if ( seg.TryGetValue( "m_container", out var contObj ) )
					{
						if ( contObj is byte[] b ) container = b;
						else if ( contObj is List<object> lo ) { container = new byte[lo.Count]; for ( int k=0;k<lo.Count;k++ ) container[k] = Convert.ToByte( lo[k] ); }
					}
					if ( container != null && container.Length >= 8 )
					{
						decoderIndex = BitConverter.ToInt16( container, 0 );
						short numElements = BitConverter.ToInt16( container, 4 );
						if ( 8 + numElements * 2 <= container.Length )
						{
							string decoder = string.Empty;
							if ( decoderIndex >= 0 && decoderIndex < decoderNames.Count ) decoder = decoderNames[decoderIndex];
							
							// Extract element IDs
							var elements = new short[numElements];
							for ( int e = 0; e < numElements; e++ ) elements[e] = BitConverter.ToInt16( container, 8 + e*2 );
							
							// Classify by decoder type
							if ( decoder.Contains("Vector3") || decoder.Contains("Float") )
							{
								foreach ( var elem in elements ) positionElements.Add( elem );
							}
							else if ( decoder.Contains("Quaternion") )
							{
								foreach ( var elem in elements ) angleElements.Add( elem );
							}
						}
					}
				}
				
				// Create Position channel (channel 0) with discovered elements
				channelAttrs.Add( "position" );
				int[] posRemap = new int[skeletonBoneCount];
				for ( int i = 0; i < posRemap.Length; i++ ) posRemap[i] = -1; // Default: no mapping
				var posElementsList = positionElements.OrderBy(x => x).ToArray();
				
				// Map elements directly: element index -> bone index for positions
				for ( int boneIdx = 0; boneIdx < skeletonBoneCount && boneIdx < posElementsList.Length; boneIdx++ )
				{
					posRemap[boneIdx] = boneIdx; // Direct mapping: bone 0 uses element 0, bone 1 uses element 1, etc.
				}
				channelRemaps.Add( posRemap );
				
				// Create Angle channel (channel 1) with discovered elements  
				channelAttrs.Add( "angle" );
				int[] angleRemap = new int[skeletonBoneCount];
				for ( int i = 0; i < angleRemap.Length; i++ ) angleRemap[i] = -1; // Default: no mapping
				var angleElementsList = angleElements.OrderBy(x => x).ToArray();
				
				// Map elements: angle elements typically start after position elements
				// If we have elements 130-259 for angles, map them to bones 0-88
				int angleElementOffset = angleElementsList.Length > 0 ? angleElementsList[0] : 0;
				for ( int boneIdx = 0; boneIdx < skeletonBoneCount; boneIdx++ )
				{
					int targetElement = angleElementOffset + boneIdx;
					if ( angleElements.Contains((short)targetElement) )
					{
						angleRemap[boneIdx] = targetElement;
					}
				}
				channelRemaps.Add( angleRemap );
				
				//Log.Info($"[HLA] Created channels from segment analysis: Position elements={string.Join(",", posElementsList)} Angle elements={string.Join(",", angleElementsList)}");
				//Log.Info($"[HLA] Skeleton has {skeletonBoneCount} bones, Position elements: {posElementsList.Length}, Angle elements: {angleElementsList.Length}");
				//Log.Info($"[HLA] Position remap: bone0->elem{posRemap[0]}, bone1->elem{posRemap[1]}, bone2->elem{posRemap[2]}");
				//Log.Info($"[HLA] Angle remap: bone0->elem{angleRemap[0]}, bone1->elem{angleRemap[1]}, bone2->elem{angleRemap[2]} (offset={angleElementOffset})");
				}
			}

			// Build segment specs
			var segData = new List<(string decoder, byte[] data, int elementCount, int[] wantedElements, int[] remapBones, string attr)>();
			for ( int i = 0; i < segmentsList.Count; i++ )
			{
				if ( segmentsList[i] is not Dictionary<string,object> seg ) { segData.Add( (string.Empty, Array.Empty<byte>(), 0, Array.Empty<int>(), Array.Empty<int>(), string.Empty) ); continue; }
				int localChannel = GetInt( seg, "m_nLocalChannel" ) ?? -1;
				if ( localChannel < 0 || localChannel >= channelAttrs.Count ) { segData.Add( (string.Empty, Array.Empty<byte>(), 0, Array.Empty<int>(), Array.Empty<int>(), string.Empty) ); continue; }
				string attr = channelAttrs[localChannel];
				var remap = channelRemaps[localChannel];
				// container bytes
				byte[] container = null;
				if ( seg.TryGetValue( "m_container", out var contObj ) )
				{
					if ( contObj is byte[] b ) container = b;
					else if ( contObj is List<object> lo ) { container = new byte[lo.Count]; for ( int k=0;k<lo.Count;k++ ) container[k] = Convert.ToByte( lo[k] ); }
				}
				if ( container == null || container.Length < 8 ) { segData.Add( (string.Empty, Array.Empty<byte>(), 0, Array.Empty<int>(), Array.Empty<int>(), string.Empty) ); continue; }
				// header
				short decoderIndex = BitConverter.ToInt16( container, 0 );
				short numElements = BitConverter.ToInt16( container, 4 );
				int headerEnd = 8 + (numElements * 2);
				if ( headerEnd > container.Length ) { segData.Add( (string.Empty, Array.Empty<byte>(), 0, Array.Empty<int>(), Array.Empty<int>(), string.Empty) ); continue; }
				// element id list
				var elements = new short[numElements];
				for ( int e = 0; e < numElements; e++ ) elements[e] = BitConverter.ToInt16( container, 8 + e*2 );
				// wantedElements & remap bones
				var wanted = new List<int>();
				var outBones = new List<int>();
				for ( int bone = 0; bone < remap.Length; bone++ )
				{
					int elemId = remap[bone];
					if ( elemId == -1 ) continue;
					// elements[] is list of element indices (local to channel)
					int pos = -1;
					for ( int e = 0; e < elements.Length; e++ ) { if ( elements[e] == elemId ) { pos = e; break; } }
					if ( pos != -1 ) { wanted.Add( pos ); outBones.Add( bone ); }
				}
				// data after header
				var payload = new byte[ container.Length - headerEnd ];
				Buffer.BlockCopy( container, headerEnd, payload, 0, payload.Length );
				string decoder = string.Empty;
				if ( decoderIndex >= 0 && decoderIndex < decoderNames.Count ) decoder = decoderNames[decoderIndex];
				segData.Add( (decoder, payload, numElements, wanted.ToArray(), outBones.ToArray(), attr) );
					//Log.Info($"[HLA] seg[{i}] dec={decoder} attr={attr} elems={numElements} wanted={wanted.Count} bones={outBones.Count} dataLen={payload.Length} localChan={localChannel}");
			}

			int added = 0;
			foreach ( var animObj in animList )
			{
				if ( animObj is not Dictionary<string,object> anim ) continue;
				var name = GetString( anim, "m_name" ) ?? "anim";
				float fps = GetFloat( anim, "fps" ) ?? 30f;
				if ( !anim.TryGetValue( "m_pData", out var pdataObj ) || pdataObj is not Dictionary<string,object> pdata ) continue;
				int frames = GetInt( pdata, "m_nFrames" ) ?? 0;
				if ( frames <= 0 ) continue;
				// frame blocks for this anim
				var blocks = new List<(int start, int end, int[] segIdx)>();
				if ( pdata.TryGetValue( "m_frameblockArray", out var fbaObj ) && fbaObj is List<object> fbList )
				{
					foreach ( var fbObj in fbList )
					{
						if ( fbObj is not Dictionary<string,object> fb ) continue;
						int start = GetInt( fb, "m_nStartFrame" ) ?? 0;
						int end = GetInt( fb, "m_nEndFrame" ) ?? -1;
						int[] segIdx = Array.Empty<int>();
						if ( fb.TryGetValue( "m_segmentIndexArray", out var siaObj ) && siaObj is List<object> siaList )
						{
							segIdx = new int[siaList.Count];
							for ( int k=0;k<siaList.Count;k++ ) segIdx[k] = Convert.ToInt32( siaList[k] );
						}
						blocks.Add( (start, end, segIdx) );
					}
				}
				if ( blocks.Count == 0 ) continue;

				var builderAnim = builder.AddAnimation( name, (int)MathF.Round( fps ) );
				// Map flags
				if ( anim.TryGetValue("m_flags", out var flagsObj) && flagsObj is Dictionary<string,object> flagsDict )
				{
					if ( GetBool(flagsDict, "m_bLooping") ) builderAnim.WithLooping(true);
					if ( GetBool(flagsDict, "m_bDelta") ) builderAnim.WithDelta(true);
				}
				Log.Info($"[HLA] anim '{name}': frames={frames} fps={fps:F1} segments={segData.Count}");
				for ( int f = 0; f < frames; f++ )
				{
					var local = new Transform[skeletonBoneCount];
					for ( int i = 0; i < skeletonBoneCount; i++ ) local[i] = new Transform( Vector3.Zero, Rotation.Identity, 1f );
					var posSet = new bool[skeletonBoneCount];
					var rotSet = new bool[skeletonBoneCount];
					var scaleSet = new bool[skeletonBoneCount];

					foreach ( var (start,end,segIdx) in blocks )
					{
						if ( f < start || (end >= 0 && f > end) ) continue;
						for ( int s = 0; s < segIdx.Length; s++ )
						{
							int si = segIdx[s];
							if ( si < 0 || si >= segData.Count ) continue;
							ApplySegmentToTransforms( segData[si], f - start, ref local, posSet, rotSet, scaleSet );
						}
					}

					// Pass LOCAL transforms: fall back to bind local when channel missing
					var localOut = new Transform[skeletonBoneCount];
					for ( int i = 0; i < skeletonBoneCount; i++ )
					{
						var basePos = (bindLocalPos != null && i < bindLocalPos.Length) ? bindLocalPos[i] : Vector3.Zero;
						var baseRot = (bindLocalRot != null && i < bindLocalRot.Length) ? bindLocalRot[i] : Rotation.Identity;
						var li = local[i];
						var lp = posSet[i] ? li.Position : basePos;
						var lr = rotSet[i] ? li.Rotation.Normal : baseRot;
						localOut[i] = new Transform( lp, lr, 1f );
					}
					if ( f < 2 )
					{
						int pc = 0, rc = 0;
						for ( int i = 0; i < skeletonBoneCount; i++ ) { if (posSet[i]) pc++; if (rotSet[i]) rc++; }
						Log.Info($"[HLA] frame {f}: posSet={pc} rotSet={rc} firstBone pos={(localOut[0].Position)} rot={(localOut[0].Rotation)}");
						
						// Debug: Show first few bone transforms in detail
						if ( f == 0 )
						{
							for ( int debugBone = 0; debugBone < Math.Min( 5, localOut.Length ); debugBone++ )
							{
								var t = localOut[debugBone];
								bool posWasSet = debugBone < posSet.Length && posSet[debugBone];
								bool rotWasSet = debugBone < rotSet.Length && rotSet[debugBone];
								Log.Info($"[HLA] bone[{debugBone}] pos=({t.Position.x:F3},{t.Position.y:F3},{t.Position.z:F3}) rot=({t.Rotation.x:F3},{t.Rotation.y:F3},{t.Rotation.z:F3},{t.Rotation.w:F3}) posSet={posWasSet} rotSet={rotWasSet}");
							}
						}
					}

					builderAnim.AddFrame( localOut );
				}
				added++;
			}

			return added;
		}

		// Extremely small legacy text KV parser to find AnimationClip-like data: m_animArray[] with m_name, fps, m_pData.m_nFrames
		private static int TryParseLegacyAnimKVText( ModelBuilder builder, string text, Dictionary<string,int> boneNameToIndex, int skeletonBoneCount )
		{
			// This is a placeholder to confirm presence; robust segment decoding is a follow-up
			int count = 0;
			int idx = 0;
			while ( true )
			{
				int a = text.IndexOf("m_animArray", idx, StringComparison.Ordinal);
				if ( a < 0 ) break;
				int namePos = text.IndexOf("m_name", a, StringComparison.Ordinal);
				if ( namePos < 0 ) break;
				int q0 = text.IndexOf('"', namePos+6);
				int q1 = q0 >= 0 ? text.IndexOf('"', q0+1) : -1;
				string animName = (q0>=0 && q1>q0) ? text.Substring(q0+1, q1-q0-1) : $"anim_{count}";
				int fpsPos = text.IndexOf("fps", namePos, StringComparison.Ordinal);
				float fps = 30f;
				if ( fpsPos > 0 )
				{
					var m0 = System.Text.RegularExpressions.Regex.Match(text.Substring(fpsPos), @"fps\s*=\s*([0-9]+(\.[0-9]+)?)");
					if ( m0.Success ) float.TryParse(m0.Groups[1].Value, out fps);
				}
				int framesPos = text.IndexOf("m_nFrames", namePos, StringComparison.Ordinal);
				int numFrames = 0;
				if ( framesPos > 0 )
				{
					var m1 = System.Text.RegularExpressions.Regex.Match(text.Substring(framesPos), @"m_nFrames\s*=\s*([0-9]+)");
					if ( m1.Success ) int.TryParse(m1.Groups[1].Value, out numFrames);
				}
				if ( numFrames > 0 )
				{
					var anim = builder.AddAnimation( animName, (int)MathF.Round(fps) );
					for ( int f = 0; f < numFrames; f++ )
					{
						var transforms = new Transform[skeletonBoneCount];
						for ( int i = 0; i < skeletonBoneCount; i++ ) transforms[i] = new Transform( Vector3.Zero, Rotation.Identity );
						anim.AddFrame( transforms );
					}
					count++;
				}
				idx = a + 10;
			}
			if ( count > 0 ) Log.Info($"[HLA] Legacy anim KV text: stub-added {count} animations (no motion yet)");
			return count;
		}

		private static void CollectAnimStringsRecursive( object node, HashSet<string> output )
		{
			if ( node == null ) return;
			if ( node is string s )
			{
				s = s.Trim();
				if ( s.StartsWith("resource:", StringComparison.OrdinalIgnoreCase) ) s = s.Substring(9);
				if ( s.EndsWith(".vanim", StringComparison.OrdinalIgnoreCase) || s.EndsWith(".vanim_c", StringComparison.OrdinalIgnoreCase) ||
					 s.EndsWith(".vseq", StringComparison.OrdinalIgnoreCase) || s.EndsWith(".vseq_c", StringComparison.OrdinalIgnoreCase) )
				{
					output.Add( s.Replace('\\','/').ToLowerInvariant() );
				}
				return;
			}
			if ( node is Dictionary<string,object> dict )
			{
				foreach ( var v in dict.Values ) CollectAnimStringsRecursive( v, output );
				return;
			}
			if ( node is List<object> list )
			{
				foreach ( var v in list ) CollectAnimStringsRecursive( v, output );
				return;
			}
		}

		private string NormalizeCompiledPath( string path )
		{
			var p = path.Replace('\\','/').ToLowerInvariant();
			if ( p.StartsWith("resource:", StringComparison.OrdinalIgnoreCase) ) p = p.Substring(9);
			if ( !p.EndsWith("_c", StringComparison.Ordinal ) )
			{
				if ( p.EndsWith(".vanim", StringComparison.OrdinalIgnoreCase) || p.EndsWith(".vseq", StringComparison.OrdinalIgnoreCase) || p.EndsWith(".vmdl", StringComparison.OrdinalIgnoreCase) )
				{
					p += "_c";
				}
			}
			return p;
		}

		private void LoadVanimAndAddToModel( ModelBuilder builder, string animPath, int skeletonBoneCount )
		{
			var compiled = NormalizeCompiledPath( animPath );
			if ( !compiled.EndsWith(".vanim_c", StringComparison.OrdinalIgnoreCase) ) return;

			var bytes = base.Host.GetFileBytes( compiled );
			using var ms = new MemoryStream( bytes );
			using var br = new BinaryReader( ms );
			uint _fileSize = br.ReadUInt32();
			ushort _headerVer = br.ReadUInt16();
			ushort _typeVer = br.ReadUInt16();
			uint blockOffset = br.ReadUInt32();
			uint blockCount = br.ReadUInt32();

			ms.Position += blockOffset - 8;
			uint dataOfs = 0, dataSize = 0;
			for ( int i = 0; i < blockCount; i++ )
			{
				uint blockType = br.ReadUInt32();
				long pos = ms.Position;
				uint rel = br.ReadUInt32();
				uint size = br.ReadUInt32();
				if ( FourCC(blockType) == "DATA" )
				{
					dataOfs = (uint)(pos + rel);
					dataSize = size;
				}
				ms.Position = pos + 8;
			}
			if ( dataSize == 0 ) throw new InvalidDataException("vanim DATA block missing");

			ms.Position = dataOfs;
			var dataBlob = br.ReadBytes( (int)dataSize );
			var kv = BinaryKV3Lite.Parse( dataBlob );
			if ( kv == null ) throw new InvalidDataException("vanim DATA parse failed");

			int numFrames = GetInt( kv, "m_nNumFrames" ) ?? 0;
			float duration = GetFloat( kv, "m_flDuration" ) ?? 0.0f;
			float fps = (duration > 0 && numFrames > 0) ? (numFrames / duration) : 30.0f;
			string animName = System.IO.Path.GetFileNameWithoutExtension( animPath );

			var settings = new List<TrackSetting>();
			if ( kv.TryGetValue( "m_trackCompressionSettings", out var tcsObj ) && tcsObj is List<object> tcs )
			{
				foreach ( var entry in tcs )
				{
					if ( entry is not Dictionary<string,object> d ) continue;
					var tx = GetRange( d, "m_translationRangeX" );
					var ty = GetRange( d, "m_translationRangeY" );
					var tz = GetRange( d, "m_translationRangeZ" );
					var sc = GetRange( d, "m_scaleRange" );
					var constRotArr = GetFloatArray( d, "m_constantRotation" );
					var constRot = (constRotArr != null && constRotArr.Length >= 4) ? new Rotation( constRotArr[0], constRotArr[1], constRotArr[2], constRotArr[3] ) : Rotation.Identity;
					bool isRotStatic = GetBool( d, "m_bIsRotationStatic" );
					bool isTrStatic = GetBool( d, "m_bIsTranslationStatic" );
					bool isScStatic = GetBool( d, "m_bIsScaleStatic" );
					settings.Add( new TrackSetting { TRX = tx, TRY = ty, TRZ = tz, Scale = sc, ConstantRotation = constRot, IsRotationStatic = isRotStatic, IsTranslationStatic = isTrStatic, IsScaleStatic = isScStatic } );
				}
			}

			byte[] compressedData = null;
			if ( kv.TryGetValue( "m_compressedPoseData", out var cpdObj ) )
			{
				if ( cpdObj is byte[] bArr ) compressedData = bArr;
				else if ( cpdObj is List<object> list )
				{
					compressedData = new byte[list.Count];
					for ( int i = 0; i < list.Count; i++ ) compressedData[i] = Convert.ToByte( list[i] );
				}
			}
			if ( compressedData == null ) throw new InvalidDataException("m_compressedPoseData missing");

			int[] offsets = null;
			if ( kv.TryGetValue( "m_compressedPoseOffsets", out var offObj ) )
			{
				if ( offObj is List<object> l )
				{
					offsets = new int[l.Count];
					for ( int i = 0; i < l.Count; i++ ) offsets[i] = Convert.ToInt32( l[i] );
				}
			}
			if ( offsets == null || offsets.Length != numFrames ) Log.Warning("[HLA] vanim offsets missing or mismatched");

			var animationBuilder = builder.AddAnimation( animName, (int)MathF.Round( fps ) );
			var dataU16 = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, ushort>( compressedData );

			int trackCount = settings.Count;
			int boneCount = Math.Min( skeletonBoneCount, trackCount );
			for ( int frame = 0; frame < numFrames; frame++ )
			{
				var transforms = new Transform[skeletonBoneCount];
				for ( int i = 0; i < skeletonBoneCount; i++ ) transforms[i] = new Transform( Vector3.Zero, Rotation.Identity );

				int baseIndex = (offsets != null && frame < offsets.Length) ? offsets[frame] : 0;
				int di = baseIndex;

				for ( int i = 0; i < boneCount; i++ )
				{
					var cfg = settings[i];
					var pos = new Vector3( cfg.TRX.Start, cfg.TRY.Start, cfg.TRZ.Start );
					var rot = cfg.ConstantRotation;
					float scl = cfg.Scale.Start;

					if ( !cfg.IsRotationStatic ) rot = DecodeQuat15( dataU16, ref di );
					if ( !cfg.IsTranslationStatic )
					{
						pos = new Vector3(
							DecodeUnormToRange( dataU16[di++], cfg.TRX.Start, cfg.TRX.Length ),
							DecodeUnormToRange( dataU16[di++], cfg.TRY.Start, cfg.TRY.Length ),
							DecodeUnormToRange( dataU16[di++], cfg.TRZ.Start, cfg.TRZ.Length )
						);
					}
					if ( !cfg.IsScaleStatic ) scl = DecodeUnormToRange( dataU16[di++], cfg.Scale.Start, cfg.Scale.Length );

					transforms[i] = new Transform( pos, rot );
				}

				animationBuilder.AddFrame( transforms );
			}
		}

		private struct Range { public float Start; public float Length; }
		private struct TrackSetting
		{
			public Range TRX; public Range TRY; public Range TRZ; public Range Scale;
			public Rotation ConstantRotation;
			public bool IsRotationStatic, IsTranslationStatic, IsScaleStatic;
		}

		// Apply one legacy segment to transforms array (Position/Angle/Scale supported)
		private static void ApplySegmentToTransforms( (string decoder, byte[] data, int elementCount, int[] wantedElements, int[] remapBones, string attr) seg, int frameIndex, ref Transform[] transforms, bool[] posSet, bool[] rotSet, bool[] scaleSet )
		{
			if ( string.IsNullOrEmpty(seg.decoder) || seg.wantedElements.Length == 0 || seg.remapBones.Length == 0 ) return;
			
			var wanted = seg.wantedElements;
			var bones = seg.remapBones;
			string attr = seg.attr;
			var data = seg.data;

			// Static decoders don't use frame indexing, dynamic ones do
			bool isStatic = seg.decoder.Contains("Static");
			int offset = isStatic ? 0 : Math.Max(0, frameIndex) * seg.elementCount;

			// Debug first few segments
			if ( frameIndex < 2 && bones.Length > 0 )
			{
				// Log.Info($"[HLA] Applying seg {seg.decoder} attr={attr} frame={frameIndex} offset={offset} elementCount={seg.elementCount} bones={bones.Length} dataLen={data.Length}");
			}

			// Quaternion decoders (Angle attribute)
			if ( seg.decoder == "CCompressedAnimQuaternion" || seg.decoder == "CCompressedFullQuaternion" || seg.decoder == "CCompressedStaticQuaternion" )
			{
				if ( attr != "angle" ) return;
				
				if ( seg.decoder == "CCompressedAnimQuaternion" || seg.decoder == "CCompressedStaticQuaternion" )
				{
					// Compressed quaternion (6 bytes)
					int stride = 6;
					for ( int i = 0; i < bones.Length; i++ )
					{
						int elementIndex = wanted[i];
						int dataIndex = isStatic ? elementIndex * stride : (offset + elementIndex) * stride;
						if ( dataIndex + stride > data.Length ) continue;
						var q = ReadCompressedQuat6( new ReadOnlySpan<byte>( data, dataIndex, stride ) );
						int bone = bones[i];
						
						// Debug: Check if same quaternion is being applied to all bones
						if ( frameIndex <= 1 && bone <= 10 )
						{
							var rawBytes = new ReadOnlySpan<byte>( data, dataIndex, stride );
							//Log.Info($"[HLA] bone[{bone}] elem={elementIndex} dataIdx={dataIndex} quat=({q.x:F3},{q.y:F3},{q.z:F3},{q.w:F3}) rawBytes=[{rawBytes[0]},{rawBytes[1]},{rawBytes[2]},{rawBytes[3]},{rawBytes[4]},{rawBytes[5]}]");
						}
						
						transforms[bone] = new Transform( transforms[bone].Position, q );
						if ( rotSet != null && bone >= 0 && bone < rotSet.Length ) rotSet[bone] = true;
					}
				}
				else // CCompressedFullQuaternion
				{
					// Full quaternion (16 bytes)
					int stride = 16;
					for ( int i = 0; i < bones.Length; i++ )
					{
						int elementIndex = wanted[i];
						int dataIndex = (offset + elementIndex) * stride;
						if ( dataIndex + stride > data.Length ) continue;
						float x = BitConverter.ToSingle(data, dataIndex+0);
						float y = BitConverter.ToSingle(data, dataIndex+4);
						float z = BitConverter.ToSingle(data, dataIndex+8);
						float w = BitConverter.ToSingle(data, dataIndex+12);
						int bone = bones[i];
						transforms[bone] = new Transform( transforms[bone].Position, new Rotation(x, y, z, w) );
						if ( rotSet != null && bone >= 0 && bone < rotSet.Length ) rotSet[bone] = true;
					}
				}
				return;
			}

			// Vector3 decoders (Position attribute)
			if ( seg.decoder == "CCompressedAnimVector3" || seg.decoder == "CCompressedFullVector3" || seg.decoder == "CCompressedDeltaVector3" || 
				 seg.decoder == "CCompressedStaticVector3" || seg.decoder == "CCompressedStaticFullVector3" )
			{
				if ( attr != "position" ) return;
				
				if ( seg.decoder == "CCompressedAnimVector3" || seg.decoder == "CCompressedStaticVector3" )
				{
					// Half3 (6 bytes)
					int stride = 6;
					for ( int i = 0; i < bones.Length; i++ )
					{
						int elementIndex = wanted[i];
						int dataIndex = isStatic ? elementIndex * stride : (offset + elementIndex) * stride;
						if ( dataIndex + stride > data.Length ) continue;
						var vx = (float)BitConverter.ToHalf(data, dataIndex+0);
						var vy = (float)BitConverter.ToHalf(data, dataIndex+2);
						var vz = (float)BitConverter.ToHalf(data, dataIndex+4);
						int bone = bones[i];
						transforms[bone] = new Transform( new Vector3(vx,vy,vz), transforms[bone].Rotation );
						if ( posSet != null && bone >= 0 && bone < posSet.Length ) posSet[bone] = true;
					}
				}
				else if ( seg.decoder == "CCompressedFullVector3" || seg.decoder == "CCompressedStaticFullVector3" )
				{
					// Full Vector3 (12 bytes)
					int stride = 12;
					for ( int i = 0; i < bones.Length; i++ )
					{
						int elementIndex = wanted[i];
						int dataIndex = isStatic ? elementIndex * stride : (offset + elementIndex) * stride;
						if ( dataIndex + stride > data.Length ) continue;
						float x = BitConverter.ToSingle(data, dataIndex+0);
						float y = BitConverter.ToSingle(data, dataIndex+4);
						float z = BitConverter.ToSingle(data, dataIndex+8);
						int bone = bones[i];
						transforms[bone] = new Transform( new Vector3(x,y,z), transforms[bone].Rotation );
						if ( posSet != null && bone >= 0 && bone < posSet.Length ) posSet[bone] = true;
					}
				}
				else // CCompressedDeltaVector3
				{
					// Layout: base vectors followed by frames of half3 deltas
					int baseSize = seg.elementCount * 12; // float3 per element
					int deltaStride = 6; // Half3 per element per frame
					for ( int i = 0; i < bones.Length; i++ )
					{
						int elementIndex = wanted[i];
						int baseIndex = elementIndex * 12;
						if ( baseIndex + 12 > data.Length ) continue;
						float bx = BitConverter.ToSingle(data, baseIndex+0);
						float by = BitConverter.ToSingle(data, baseIndex+4);
						float bz = BitConverter.ToSingle(data, baseIndex+8);
						int deltaIndex = baseSize + (offset + elementIndex) * deltaStride;
						if ( deltaIndex + deltaStride > data.Length ) continue;
						float dx = (float)BitConverter.ToHalf(data, deltaIndex+0);
						float dy = (float)BitConverter.ToHalf(data, deltaIndex+2);
						float dz = (float)BitConverter.ToHalf(data, deltaIndex+4);
						int bone = bones[i];
						transforms[bone] = new Transform( new Vector3(bx+dx, by+dy, bz+dz), transforms[bone].Rotation );
						if ( posSet != null && bone >= 0 && bone < posSet.Length ) posSet[bone] = true;
					}
				}
				return;
			}

			// Float decoders (Scale attribute)
			if ( seg.decoder == "CCompressedFullFloat" || seg.decoder == "CCompressedStaticFloat" )
			{
				if ( attr != "scale" ) return;
				int stride = 4;
				for ( int i = 0; i < bones.Length; i++ )
				{
					int elementIndex = wanted[i];
					int dataIndex = isStatic ? elementIndex * stride : (offset + elementIndex) * stride;
					if ( dataIndex + stride > data.Length ) continue;
					float s = BitConverter.ToSingle(data, dataIndex);
					int bone = bones[i];
					var t = transforms[bone];
					transforms[bone] = new Transform( t.Position, t.Rotation, s );
					if ( scaleSet != null && bone >= 0 && bone < scaleSet.Length ) scaleSet[bone] = true;
				}
				return;
			}
		}

		private static Range GetRange( Dictionary<string,object> obj, string key )
		{
			if ( obj.TryGetValue( key, out var v ) && v is Dictionary<string,object> r )
			{
				var start = GetFloat( r, "m_flRangeStart" ) ?? 0f;
				var len = GetFloat( r, "m_flRangeLength" ) ?? 0f;
				return new Range{ Start = start, Length = len };
			}
			return new Range{ Start = 0, Length = 0 };
		}

		private static bool GetBool( Dictionary<string,object> obj, string key )
		{
			if ( obj != null && obj.TryGetValue( key, out var v ) )
			{
				try { return Convert.ToBoolean( v ); } catch { }
			}
			return false;
		}

		private static Rotation DecodeQuat15( ReadOnlySpan<ushort> data, ref int idx )
		{
			float valueRangeMin = -1.0f / MathF.Sqrt( 2.0f );
			float valueRangeMax = 1.0f / MathF.Sqrt( 2.0f );
			float mul = (valueRangeMax - valueRangeMin) / 0x7FFF;

			ushort a = data[idx++];
			ushort b = data[idx++];
			ushort c = data[idx++];

			float x = (a & 0x7FFF) * mul + valueRangeMin;
			float y = (b & 0x7FFF) * mul + valueRangeMin;
			float z = (float)c * mul + valueRangeMin;
			float sum = x * x + y * y + z * z;
			float w = MathF.Sqrt( MathF.Max( 0f, 1f - sum ) );

			ushort largest = (ushort)(((a >> 14) & 0x0002) | (b >> 15));
			switch ( largest )
			{
				case 0: return new Rotation( x, y, z, w );
				case 1: return new Rotation( w, y, z, x );
				case 2: return new Rotation( x, w, z, y );
				case 3: return new Rotation( x, y, w, z );
			}
			return Rotation.Identity;
		}

		// Local copy of VRF's 6-byte compressed quaternion reader returning Rotation
		private static Rotation ReadCompressedQuat6( ReadOnlySpan<byte> bytes )
		{
			var i1 = bytes[0] + ((bytes[1] & 63) << 8);
			var i2 = bytes[2] + ((bytes[3] & 63) << 8);
			var i3 = bytes[4] + ((bytes[5] & 63) << 8);
			var s1 = bytes[1] & 128;
			var s2 = bytes[3] & 128;
			var s3 = bytes[5] & 128;
			var c = MathF.Sin(MathF.PI / 4.0f) / 16384.0f;
			var x = (bytes[1] & 64) == 0 ? c * (i1 - 16384) : c * i1;
			var y = (bytes[3] & 64) == 0 ? c * (i2 - 16384) : c * i2;
			var z = (bytes[5] & 64) == 0 ? c * (i3 - 16384) : c * i3;
			var w = MathF.Sqrt(MathF.Max(0f, 1 - (x * x) - (y * y) - (z * z)));
			if (s3 == 128) w *= -1;
			
			// Component swizzling based on which component was dropped during compression
			// s&box Rotation constructor expects (x, y, z, w) order
			if (s1 == 128)
			{
				return s2 == 128 ? new Rotation(y, z, w, x) : new Rotation(z, w, x, y);
			}
			return s2 == 128 ? new Rotation(w, x, y, z) : new Rotation(x, y, z, w);
		}

		private static float DecodeUnormToRange( ushort v, float start, float length )
		{
			return (v / 65535.0f) * length + start;
		}
		
		private static List<bool> ParseBitArray( List<object> bitArray )
		{
			var result = new List<bool>();
			
			foreach ( var byteObj in bitArray )
			{
				if ( byteObj is not long byteValue ) continue;
				byte b = (byte)byteValue;
				
				// Extract 8 bits from this byte
				for ( int bit = 0; bit < 8; bit++ )
				{
					result.Add( (b & (1 << bit)) != 0 );
				}
			}
			
			return result;
		}
	}
}


