
#pragma once

#include "CoreMinimal.h"
#include "Engine/DataAsset.h"
#include "KafkaDataAsset.generated.h"

class UKafkaParserBase;


 /*KafkaStruct
  *配合方法ImportKafkaConfig()使用
  */
USTRUCT(BlueprintType)
struct FKafkaConfig
{
	GENERATED_BODY()
public:
	//区分配置和业务数据：例如：通用设置、运动、UI
	UPROPERTY(BlueprintReadWrite, Category = "KafkaConfig")
		FString Type;
	UPROPERTY(BlueprintReadWrite, Category = "KafkaConfig")
		FName Topic;
	UPROPERTY(BlueprintReadWrite, Category = "KafkaConfig")
		float RefreshTimer;
	UPROPERTY(BlueprintReadWrite, Category = "KafkaConfig")
		TMap<FString, FString> ConfigSettings;
};

//外部对象数组
USTRUCT(BlueprintType)
struct FKafkaOuterObject
{
	GENERATED_BODY()
public:
	UPROPERTY()
		TArray<TWeakObjectPtr<UObject>> Outers;
};

//KafkaProjectSettings
UCLASS()
class DMHLKAFKA_API UKafkaDataAsset : public UPrimaryDataAsset
{
	GENERATED_BODY()
public:
	//
	UPROPERTY(EditDefaultsOnly, BlueprintReadOnly, Category = "KafkaParser")
		FString OutSettingPath;
	//Topic2ParserClass
	UPROPERTY(EditDefaultsOnly, BlueprintReadOnly, Category = "KafkaParser")
		TMap<FString, TSubclassOf<class UKafkaParserBase>> Topic2KafkaParser;


};

