using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Sandbox
{
	/// <summary>
	/// Simple REDI block parser focused on extracting SpecialDependencies for texture conversions
	/// Based on ValveResourceFormat's ResourceEditInfo.cs
	/// </summary>
	internal static class REDIParser
	{
		public class SpecialDependency
		{
			public string CompilerIdentifier { get; set; }
			public uint Fingerprint { get; set; }
			public string String { get; set; }
			public uint UserData { get; set; }

			public SpecialDependency(BinaryReader reader)
			{
				var startPos = reader.BaseStream.Position;
				// VRF field order: String, CompilerIdentifier, Fingerprint, UserData
				String = ReadOffsetString(reader);
				CompilerIdentifier = ReadOffsetString(reader);
				Fingerprint = reader.ReadUInt32();
				UserData = reader.ReadUInt32();
				
				// Log.Info($"[HLA] REDI Debug - Position: {startPos}, CompilerIdentifier: '{CompilerIdentifier}', Fingerprint: {Fingerprint}, String: '{String}', UserData: {UserData}");
			}
		}

		/// <summary>
		/// Parse REDI block and extract SpecialDependencies as a simplified dictionary structure
		/// </summary>
		public static Dictionary<string, object> ParseREDI(byte[] rediBytes)
		{
			using var ms = new MemoryStream(rediBytes);
			using var reader = new BinaryReader(ms);

			var result = new Dictionary<string, object>();
			var specialDependencies = new List<SpecialDependency>();

			try
			{
				// REDI structure has multiple sub-blocks with offset/count pairs
				// We need to find the SpecialDependencies sub-block (index 3)
				
				var subBlock = 0;
				
				// Skip first 3 sub-blocks (InputDependencies, AdditionalInputDependencies, ArgumentDependencies)
				for (int i = 0; i < 3; i++)
				{
					SkipSubBlock(reader, ref subBlock);
				}
				
				// Read SpecialDependencies (sub-block 3)
				var count = AdvanceGetCount(reader, ref subBlock, rediBytes.Length);
				
				for (var i = 0; i < count; i++)
				{
					try
					{
						var dependency = new SpecialDependency(reader);
						specialDependencies.Add(dependency);
					}
					catch (Exception ex)
					{
						// Log.Warning($"[HLA] Failed to read SpecialDependency {i}: {ex.Message}");
						break;
					}
				}

				// Convert to dictionary structure for compatibility
				var specialDepsDict = new Dictionary<string, object>();
				for (int i = 0; i < specialDependencies.Count; i++)
				{
					var dep = specialDependencies[i];
					var depDict = new Dictionary<string, object>
					{
						["CompilerIdentifier"] = dep.CompilerIdentifier,
						["Fingerprint"] = dep.Fingerprint,
						["String"] = dep.String,
						["UserData"] = dep.UserData
					};
					specialDepsDict[i.ToString()] = depDict;
				}

				result["SpecialDependencies"] = specialDepsDict;
				
				// Log.Info($"[HLA] Parsed REDI block: {specialDependencies.Count} SpecialDependencies");
				foreach (var dep in specialDependencies)
				{
					// Log.Info($"[HLA] SpecialDependency: CompilerIdentifier='{dep.CompilerIdentifier}', String='{dep.String}', Fingerprint={dep.Fingerprint}");
				}
			}
			catch (Exception ex)
			{
				// Log.Warning($"[HLA] Failed to parse REDI block: {ex.Message}");
			}

			return result;
		}

		private static void SkipSubBlock(BinaryReader reader, ref int subBlock)
		{
			var basePos = reader.BaseStream.Position;
			reader.BaseStream.Position = subBlock * 8;

			var offset = reader.ReadUInt32();
			var count = reader.ReadUInt32();

			// Skip to next sub-block
			subBlock++;
			reader.BaseStream.Position = basePos;
		}

		private static int AdvanceGetCount(BinaryReader reader, ref int subBlock, long totalSize)
		{
			reader.BaseStream.Position = subBlock * 8;

			var offset = reader.ReadUInt32();
			var count = reader.ReadUInt32();

			// Validate offset
			if (offset >= totalSize)
			{
				// Log.Warning($"[HLA] Invalid REDI offset: {offset} >= {totalSize}");
				return 0;
			}

			reader.BaseStream.Position = (subBlock * 8) + offset;
			subBlock++;
			
			return (int)count;
		}

		private static string ReadOffsetString(BinaryReader reader)
		{
			var offset = reader.ReadUInt32();
			if (offset == 0)
				return string.Empty;

			var currentPos = reader.BaseStream.Position;
			reader.BaseStream.Position = currentPos - 4 + offset;

			var stringBytes = new List<byte>();
			byte b;
			while ((b = reader.ReadByte()) != 0)
			{
				stringBytes.Add(b);
			}

			reader.BaseStream.Position = currentPos;
			return Encoding.UTF8.GetString(stringBytes.ToArray());
		}
	}
}