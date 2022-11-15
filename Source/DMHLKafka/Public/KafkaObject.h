// Fill out your copyright notice in the Description page of Project Settings.

#pragma once

#include "CoreMinimal.h"
#include "UObject/Object.h"

//Kafka lib
#include "rdkafkacpp.h"
#include "KafkaObject.generated.h"

using namespace RdKafka;

DECLARE_DELEGATE_ThreeParams(FKafkaCBDele, bool, const FString&, const FString&);

UCLASS()
class DMHLKAFKA_API UKafkaObject : public UObject
{
	GENERATED_BODY() 
private:
	//设置配置Config
	bool SetConfigSetting(const TPair<FString, FString>& tpair, FString& Err);
	//一个UObject对应一个消费者
	TUniquePtr<RdKafka::KafkaConsumer> kafc;
	//Topic
	TUniquePtr<RdKafka::Conf> tconf;
	//Gloabal
	TUniquePtr<RdKafka::Conf> conf;
	//错误信息
	int lastErrorCode;
	FString PrivateStr;
	// 分区，一个Topic可以切割成多个partition，存储在不同的服务器上
	int32_t partition;
	//消费者偏移
	int64_t start_offset;
	//Topic
	std::string topic_str;
	//ReceData
	TUniquePtr<RdKafka::Message> message;

public:
	UKafkaObject(const FObjectInitializer& Initializer)
	{
	};
	
	UKafkaObject()
	{};
	~UKafkaObject() 
	{
		KafkaDataCBDelegate.Unbind();
	};

	//接收数据后只给Manager，让Manager转发
	FKafkaCBDele KafkaDataCBDelegate;
	//Topic设置成功
	UPROPERTY(BlueprintReadOnly, Category = "KafkaData")
		bool topic_partition_isset = false;
	//多线程数据拉取执行条件
	UPROPERTY(BlueprintReadOnly, Category = "KafkaData")
		bool run;
	//Topic定位错误
	UPROPERTY(BlueprintReadOnly, Category = "KafkaData | Debug")
		FString TopicString;
	//消费次数定位错误
	UPROPERTY(BlueprintReadOnly, Category = "KafkaData | Debug")
		int32 ConsumerReadTime;
public:
	//创建消费者
	bool CreateConsumer(FString& Err, const TMap<FString, FString>& ConfigurationSettings, const TMap<FString, FString>& GlobConfig);
	//订阅Topic
	bool SubscribeTopic(FString topicName, FString& Err);

	//拉取并处理数据
	bool Consume(FString& err, FString& Str, int waitTime = 200);
	//停止kafka
	void Stop();

};


