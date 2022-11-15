// Copyright Epic Games, Inc. All Rights Reserved.


using System;
using System.IO;
using System.Collections.Generic;
using System.Reflection;
using System.Diagnostics;
using System.Runtime.Serialization;
using Tools.DotNETCommon;
using UnrealBuildTool;

public class librdkafka : ModuleRules
{
	public librdkafka(ReadOnlyTargetRules Target) : base(Target)
	{
        //三方库
        Type = ModuleType.External;
        //查找文件路径
        PublicIncludePaths.Add(Path.Combine(ModuleDirectory, "librdkafka.redist.1.8.2/build/native/include/librdkafka"));

        if (Target.Platform == UnrealTargetPlatform.Win64)
        {
			var basePath = Path.GetDirectoryName(RulesCompiler.GetFileNameFromType(GetType()));
            var LibraryPath = Path.Combine(basePath, "librdkafka.redist.1.8.2/build/native/lib/win/x64/win-x64-Release/v140");

            PublicAdditionalLibraries.Add(LibraryPath + "/librdkafkacpp.lib");                          // link libraries
            PublicAdditionalLibraries.Add(LibraryPath + "/librdkafka.lib");

            var BinariesPath = Path.Combine(basePath, "librdkafka.redist.1.8.2/runtimes/win-x64/native");

            string[] Mydlls = new string[] { 
                "libcrypto-1_1-x64.dll",
                "librdkafka.dll",
                "librdkafkacpp.dll",
                "libssl-1_1-x64.dll",
                "msvcp140.dll",
                "vcruntime140.dll",
                "zlib1.dll",
                "zstd.dll"
            };

            foreach (var elem in Mydlls) 
            {
                RuntimeDependencies.Add("$(TargetOutputDir)/"+ elem , Path.Combine(BinariesPath, elem));
            }

        }
    }
}
