#include "KafkaParserBase.h"

void UKafkaParserBase::BeginParser_Implementation(const FString& OData)
{
	//记录原始数据
	NativeData = OData;
	return;
}

void UKafkaParserBase::BeginParser_Native(const FString& OData)
{
	return;
}

void UKafkaParserBase::RecKafkaCBData(bool Result, const FString& ErrorData, const FString& RecData)
{
	if (Result)
	{
		NativeData = RecData;
		BeginParser_Native(RecData);
		BeginParser(RecData);
		ParserOverDelegate.ExecuteIfBound(this);
	}
	//打印错误
	UE_LOG(LogTemp, Log, TEXT("%s"), *ErrorData);
}
