// Fill out your copyright notice in the Description page of Project Settings.

#pragma once

#include "CoreMinimal.h"
#include "Subsystems/GameInstanceSubsystem.h"
//Async
#include "KafkaAsyncTask.h"


//KafkaUObject
#include "KafkaObject.h"
#include "KafkaDataAsset.h"
#include "KafkaSubsystem.generated.h"

class UKafkaParserBase;
class UKafkaDataAsset;
class FKafkaAsyncObj;
class FRunnableThread;

UCLASS()
class DMHLKAFKA_API UKafkaSubsystem : public UGameInstanceSubsystem
{
	GENERATED_BODY() 
public:
	UKafkaSubsystem() {};
	
	//KafkaSetting
	UPROPERTY(BlueprintReadOnly, Category = "Kafka")
		TArray<FKafkaConfig> KafkaConfigSetting;
	//KafkaTopic2ObjectMap
	//每一个Topic只有一个Consumer对象
	UPROPERTY(BlueprintReadWrite, Category = "Kafka")
		TMap<FString, UKafkaObject*> Topic2Kafka;
	//数据解析器单例,注意Key是解析器的Type,不是Topic,不同的主题可以用同一个解析函数,但是有各自的解析值
	UPROPERTY(BlueprintReadOnly, Category = "Kafka")
		TMap<FString, UKafkaParserBase*> Topic2KafkaParser;
	/** Read Config XXX.Txt
	* @param	Path				配置文件路径.ex:Content / Config / KafkaConfig.json
	* @param	ImportConfig		配置拉取返回的结构体.
	* @return						配置文件导入是否成功
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		bool ImportKafkaConfig(const FString& Path, TArray<FKafkaConfig>& ImportConfig);

	/** Async Consume !!!Use "AsyncReadTopicAuto" override!!!
	* @param	Topic				Kafka Topic，查找对应消费者.
	* @param	Err					Kafka 数据拉取返回的错误.
	* @param	SleepTime			Kafka 数据拉取频率，默认1秒一次.
	* @param	bIsAutoSequence		Kafka 数据拉取频率调节，默认自动调节，拉不到数据就等2秒再拉，拉到了就用上述频率拉取.
	* @return						Kafka 数据拉取返回是否成功.
	*/
	UE_DEPRECATED(4.26, "Ues AsyncReadTopicAuto() instead of this function")
		bool AsyncReadTopic(FString Topic, FString& Err, float SleepTime = 1.f, bool bIsAutoSequence = true);
	/** 异步读取数据
	* @param	Topic				Kafka Topic，查找对应消费者.
	* @param	Err					Kafka 数据拉取返回的错误.
	* @param	SleepTime			Kafka 数据拉取频率，默认1秒一次.
	* @return						Kafka 数据拉取返回是否成功.
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		bool AsyncReadTopicAuto(FString Topic, FString& Err, float SleepTime = 1.f);

	/** Close Async Thread
	* @param	Topic				Kafka Topic，查找多线程.
	* @param	Err					线程关闭失败的错误.
	* @return						是否找到该线程，且其关闭是否成功.
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		bool AsyncStopRead(FString Topic, FString& Err);

	//Sync Consume
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		bool ReadTopic(FString Topic, FString& Err, FString& Data);

	/** Create Kafka Consumer Object
	* @param	Topic				Kafka Topic，查找对应消费者.
	* @param	Err					Kafka 数据拉取返回的错误.
	* @param	SleepTime			Kafka 数据拉取频率，默认1秒一次.
	* @param	bIsAutoSequence		Kafka 数据拉取频率调节，默认自动调节，拉不到数据就等2秒再拉，拉到了就用上述频率拉取.
	* @return						Kafka 数据拉取返回是否成功.
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		UKafkaObject* CreateKafkaObject(FString& Err, FString Topic, TMap<FString, FString> ConfigurationSettings, bool& bSuccess);

	//Remove Kafka Consumer Object
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		bool RemoveKafkaObject(FString Topic);



	//~ Begin USubsystem Interface
	virtual bool ShouldCreateSubsystem(UObject* Outer) const override;
	virtual void Initialize(FSubsystemCollectionBase& Collection) override;
	virtual void Deinitialize() override;
	//~ End USubsystem Interface

	/** UObject立即订阅Topic主题消息
	* @param	Outer				外部订阅者,UI,Actor等等.
	* @param	InTopics			订阅某个主题.
	* @param	ErrorData			订阅失败消息.
	* @return						订阅是否成功.
	*/
	bool SubscribeTopics(UObject* Outer, const FString& InTopics, FString& ErrorData);

	/** UObject延迟订阅Topic主题消息,Topic解析器创建成功后有回调
	* @param	Outer				外部订阅者,UI,Actor等等.
	* @param	InTopics			订阅某个主题.
	*/
	UFUNCTION(BlueprintCallable, Category = "KafkaData")
		void SubscribeTopicsBP(UObject* Outer, const FString& InTopics);

	//排查多线程未知原因停止,导致数据消费终止的问题
	void CheckAllActiveConsumer();

	/** 开始检查,多线程停止后自动开启新线程继续读取数据
	* @param	inTime				定时器查询时间,大于1秒.
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka | Debug")
		void StartCheckConsumer(float inTime);

	//分发消息数据
	void ParserDataOver(UKafkaParserBase* InParser);

	//记录哪些Topic生成并读取数据成功了
	UPROPERTY(BlueprintReadOnly, Category = "Kafka | Debug")
		TArray<FString> SuccessfulTopic;

	/** 监测某些Topic消费了多少次
	* @param	InTopic			查询消费者的主题.
	* @Return				    消费的次数,-1没找到该主题.
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka | Debug")
		int32 CheckConsumeTime(const FString& InTopic);

	/** 生成并读取所有KafkaTopic
	* @param	ExpTopics			额外单独操作的主题.
	* @param	CreateError			创建失败的主题.
	* @param	StartError			读数据失败的主题.
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		void StartAllTopic(const TArray<FString>& ExpTopics, TArray<FString>& CreateError, TArray<FString>& StartError);

	/** 生成并读取所有KafkaTopic,生命周期全交给其他线程
		* @param	ExpTopics			额外单独操作的主题.
		* @param	CreateError			创建失败的主题.
		* @param	StartError			读数据失败的主题.
		*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		void StartAllTopicsByNewThreads(const TArray<FString>& ExpTopics, TArray<FString>& CreateError, TArray<FString>& StartError);

	/** 停止所有KafkaTopic
	*/
	UFUNCTION(BlueprintCallable, Category = "Kafka")
		void StopAllTopic();

private:
	//创建检查定时器
	UPROPERTY()
	FTimerHandle CheckConsumerTimer;
	//检查是否停止
	TMap<FString, int32> MultThreadCheckMap;
	//多线程拉取数据间隔时间
	TMap<FString, float> MultThreadSleepTime;
	//延迟注册
	void delaySubscribe(const FString& InTopic);

	//多线程拉数据
	TMap<FString, FAsyncTask<FKafkaAsyncReadTask>*> KafkaThreadMap;
	//配置Item,包含外部配置路径
	UPROPERTY()
		UKafkaDataAsset* KafkaSettingDA;
	UPROPERTY()
		TMap<FString, FString> Topic2Parser;
	//外部数据订阅者,UI、Actor等
	UPROPERTY()
		TMap<FString, FKafkaOuterObject> Topic2OuterObject;
	//外部数据等待订阅者,UI、Actor等
	UPROPERTY()
		TMap<FString, FKafkaOuterObject> Topic2PendingObject;
	//多线程kafka实例
	TMap<FString, FKafkaAsyncObj*> Topic2AsyncKafka;
	//线程实例
	TMap<FString, FRunnableThread*> Topic2KafkaThread;
};
