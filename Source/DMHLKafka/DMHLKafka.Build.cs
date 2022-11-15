// Copyright Epic Games, Inc. All Rights Reserved.


using System;
using System.IO;
using System.Collections.Generic;
using System.Reflection;
using System.Diagnostics;
using System.Runtime.Serialization;
using Tools.DotNETCommon;
using UnrealBuildTool;

public class DMHLKafka : ModuleRules
{
	public DMHLKafka(ReadOnlyTargetRules Target) : base(Target)
	{


		PCHUsage = ModuleRules.PCHUsageMode.UseExplicitOrSharedPCHs;


		PublicIncludePaths.Add(Path.Combine(ModuleDirectory, "../ThirdParty/librdkafka.redist.1.8.2/build/native/include/librdkafka"));


		PublicIncludePaths.AddRange(
			new string[] {
				// ... add public include paths required here ...
			}
			);
				
		
		PrivateIncludePaths.AddRange(
			new string[] {
				// ... add other private include paths required here ...
			}
			);
			
		
		PublicDependencyModuleNames.AddRange(
			new string[]
			{
				"Core",
				// ... add other public dependencies that you statically link with here ...
			}
			);
			
		
		PrivateDependencyModuleNames.AddRange(
			new string[]
			{
				"CoreUObject",
				"Engine",
				"Slate",
				"SlateCore",
                "Json",
                "JsonUtilities",
				"librdkafka"
				// ... add private dependencies that you statically link with here ...	
			}
			);
		
		
		DynamicallyLoadedModuleNames.AddRange(
			new string[]
			{
				// ... add any modules that your module loads dynamically here ...
			}
			);



    }
}
