
#pragma once

#include "CoreMinimal.h"
#include "Engine/DataAsset.h"
#include "KafkaInterface.generated.h"

class UKafkaParserBase;
//数据接收以及解析完毕的接口
UINTERFACE(BlueprintType)
class DMHLKAFKA_API UKafkaInterface : public UInterface
{
	GENERATED_BODY()
};

class DMHLKAFKA_API IKafkaInterface
{
	GENERATED_BODY()
public:
	UFUNCTION(BlueprintCallable, BlueprintNativeEvent, Category = "KafkaInterface")
		void RecKafkaNativeData(const FString& NativeData);
	UFUNCTION(BlueprintCallable, BlueprintNativeEvent, Category = "KafkaInterface")
		void RecKafkaParseredDataBP(const UKafkaParserBase* ParserObject);
		void RecKafkaParseredData(TWeakObjectPtr<UKafkaParserBase> ParserObject);
};


