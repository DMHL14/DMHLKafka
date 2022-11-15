// Fill out your copyright notice in the Description page of Project Settings.

#pragma once

#include "CoreMinimal.h"
#include "UObject/Object.h"
#include "KafkaParserBase.generated.h"

DECLARE_DELEGATE_OneParam(FKafkaParserOver, UKafkaParserBase*);

UCLASS(Abstract, BlueprintType, Blueprintable)
class DMHLKAFKA_API UKafkaParserBase : public UObject
{
	GENERATED_BODY() 
public:
	~UKafkaParserBase() {
		ParserOverDelegate.Unbind();
	}
	FKafkaParserOver ParserOverDelegate;
	//蓝图解析
	UFUNCTION(BlueprintCallable, BlueprintNativeEvent, Category = "KafkaData")
		void BeginParser(const FString& OData);

	//CPP解析继承重写
	virtual void BeginParser_Native(const FString& OData);

	//接收数据回调
	virtual void RecKafkaCBData(bool Result, const FString& ErrorData, const FString& RecData);

	//原始数据
	UPROPERTY(BlueprintReadOnly, Category = "KafkaData")
		FString NativeData;
};


