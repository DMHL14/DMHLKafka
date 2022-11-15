// Fill out your copyright notice in the Description page of Project Settings.

#pragma once

#include "CoreMinimal.h"
#include "HAL/Runnable.h"

//Kafka lib
#include "rdkafkacpp.h"

using namespace RdKafka;

DECLARE_DELEGATE_ThreeParams(FKafkaRecCB, bool, const FString&, const FString&);

/**
 *
 */

class FKafkaAsyncObj : public FRunnable
{
public:
	FKafkaAsyncObj(const TMap<FString, FString>& InTopicConfig, 
			const TMap<FString, FString>& InGlobConfig,
			const FString& InTopicName,
			float InSleepTime = 1.f
			):
		topicConfig(InTopicConfig),
		globConfig(InGlobConfig),
		topicName(InTopicName),
		sleepTime(InSleepTime)
	{
		
	};
	~FKafkaAsyncObj() 
	{
		delete kafkaConsumer;
		kafkaConsumer = nullptr;
		delete topicConf;
		topicConf = nullptr;
		delete globalConf;
		globalConf = nullptr;
		delete message;
		message = nullptr;
	};

	//~ Begin FRunnable Interface.
	virtual uint32 Run() override;
	virtual bool Init() override;
	virtual void Exit() override;
	virtual void Stop() override;
	//~ End FRunnable Interface

private:

	TMap<FString, FString> topicConfig;
	TMap<FString, FString> globConfig;
	FString topicName;
	float sleepTime = 0.f;



	RdKafka::KafkaConsumer*	kafkaConsumer = nullptr;
	RdKafka::Conf*	topicConf = nullptr;
	RdKafka::Conf*	globalConf = nullptr;
	RdKafka::Message*	message = nullptr;
	//CbData
	FString StrData;
	//CbErr
	FString StrError;
	
	bool bRecSuccess = false;
	
	bool SetConfigSetting(const TPair<FString, FString>& tpair, FString& Err);
	//拉取并处理数据
	bool Consume(FString& Str, int waitTime = 200);


public:
	FKafkaRecCB DelegateOnKafkaRec;
	//创建消费者
	bool CreateConsumer(const TMap<FString, FString>& ConfigurationSettings, const TMap<FString, FString>& GlobConfig);
	//订阅Topic
	bool SubscribeTopic();

	int32 ConsumerReadTime;
	FString TopicString;
	FThreadSafeBool bIsRunning = false;


};
