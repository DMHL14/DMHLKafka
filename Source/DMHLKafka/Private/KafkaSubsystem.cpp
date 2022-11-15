// Fill out your copyright notice in the Description page of Project Settings.


#include "KafkaSubsystem.h"

//Json&Path
#include "Misc/Paths.h"
#include "Serialization/JsonReader.h"
#include "Serialization/JsonSerializer.h"
//Match
#include "Kismet/KismetMathLibrary.h"

#include "KafkaAsyncObj.h"
#include "HAL/RunnableThread.h"
#include "KafkaInterface.h"
#include "KafkaParserBase.h"

bool UKafkaSubsystem::ImportKafkaConfig(const FString& path, TArray<FKafkaConfig>& Items)
{
	FString fullPath = FPaths::ProjectDir() / path;
	FString content;

	if (!FFileHelper::LoadFileToString(content, *fullPath))
	{
		UE_LOG(LogTemp, Log, TEXT("Kafka file does not exist!!!"));
		return false;
	}

	TArray<TSharedPtr<FJsonValue>> OutArray;
	TSharedRef<TJsonReader<TCHAR>> jsonReader = TJsonReaderFactory<TCHAR>::Create(*content);

	if (FJsonSerializer::Deserialize<TCHAR>(jsonReader, OutArray))
	{
		for (const auto& elem : OutArray)
		{
			const TSharedPtr<FJsonObject>& obj = elem->AsObject();
			FKafkaConfig& newItem = Items.AddDefaulted_GetRef();
			newItem.Type = obj->GetStringField(TEXT("Type"));
			FString TopicTmp = obj->GetStringField(TEXT("Topic"));
			newItem.Topic = FName(*TopicTmp);
			//外部匹配解析器
			Topic2Parser.Add(TopicTmp, newItem.Type);
			newItem.RefreshTimer = obj->GetNumberField(TEXT("RefreshTimer"));
			const TSharedPtr<FJsonObject>& Inobj = obj->GetObjectField(TEXT("Config"));
			if (Inobj.IsValid()) {
				for (const auto& Elem : Inobj->Values)
				{
					newItem.ConfigSettings.Emplace(Elem.Key, Inobj->GetStringField(Elem.Key));
				}
			}
		}
		return true;
	}
	return false;
}


bool UKafkaSubsystem::AsyncReadTopic(FString Topic, FString& Err, float SleepTime /*= 1.f*/, bool bIsAutoSequence /*= true*/)
{
	//找到KafkaObj并生成一个线程拉数据
	UKafkaObject* Tmp = Topic2Kafka.FindRef(Topic);
	if (!Tmp->IsValidLowLevel() || !Tmp->topic_partition_isset)
	{
		Err = FString(TEXT("NoTopic!"));
		return false;
	}
	Tmp->run = true;
	//开一个线程拉数据
	auto ptr = KafkaThreadMap.FindRef(Topic);
	if (!ptr)
	{
		ptr = new FAsyncTask<FKafkaAsyncReadTask>(Tmp, SleepTime, bIsAutoSequence);
		KafkaThreadMap.Emplace(Topic, ptr);
	}
	ptr->StartBackgroundTask();
	return true;
}

bool UKafkaSubsystem::AsyncReadTopicAuto(FString Topic, FString& Err, float SleepTime /*= 1.f*/)
{
	//找到KafkaObj并生成一个线程拉数据
	UKafkaObject* Tmp = Topic2Kafka.FindRef(Topic);
	if (!Tmp->IsValidLowLevel() || !Tmp->topic_partition_isset)
	{
		Err = FString(TEXT("NoTopic!"));
		return false;
	}
	Tmp->run = true;
	(new FAutoDeleteAsyncTask<FKafkaAutoAsyncTask>(Tmp, SleepTime))->StartBackgroundTask();
	return true;
}

bool UKafkaSubsystem::AsyncStopRead(FString Topic, FString& Err)
{
	//找到KafkaObj并关闭线程
	UKafkaObject* Tmp = Topic2Kafka.FindRef(Topic);
	if (!Tmp->IsValidLowLevel() || !Tmp->topic_partition_isset)
	{
		Err = FString(TEXT("NoTopic!"));
		return false;
	}
	Tmp->run = false;
	auto ptr = KafkaThreadMap.FindRef(Topic);
	//取消不了就等吧
	if (ptr != nullptr && !ptr->Cancel())
	{
		ptr->EnsureCompletion();
	}
	return true;
}

bool UKafkaSubsystem::ReadTopic(FString Topic, FString& Err, FString& Data)
{
	UKafkaObject* Tmp = Topic2Kafka.FindRef(Topic);
	if (!Tmp->IsValidLowLevel() || !Tmp->topic_partition_isset)
	{
		Err = FString(TEXT("NoTopic!"));
		return false;
	}
	bool bsuccess = Tmp->Consume(Err, Data);
	//Tmp->OnKafkaDataCB.Broadcast(bsuccess, Err, Data);
	return bsuccess;
}

UKafkaObject* UKafkaSubsystem::CreateKafkaObject(FString& Err, FString Topic, TMap<FString, FString> ConfigurationSettings, bool& bSuccess)
{
	UKafkaObject* KafkaObj = Topic2Kafka.FindRef(Topic);
	if (KafkaObj->IsValidLowLevel())
	{
		Err = FString(TEXT("Have Already Created the Consumer of the Topic!!!"));
		UE_LOG(LogTemp, Log, TEXT("%s"), *Err);
		bSuccess = true;
		return KafkaObj;
	}
	//没找到就创建
	KafkaObj = NewObject<UKafkaObject>();
	if (KafkaConfigSetting.Num() <= 0)
	{
		Err = FString(TEXT("Kafka CommonConfig is empty, can't create Consumer !!!"));
		UE_LOG(LogTemp, Log, TEXT("%s"), *Err);
		bSuccess = false;
		return nullptr;
	}
	if (!KafkaObj->CreateConsumer(Err, ConfigurationSettings, KafkaConfigSetting[0].ConfigSettings))
	{
		UE_LOG(LogTemp, Log, TEXT("%s"), *Err);
		bSuccess = false;
		return nullptr;
	}
	if (!KafkaObj->SubscribeTopic(Topic, Err))
	{
		UE_LOG(LogTemp, Log, TEXT("Subscribe Topic Error!!!"));
		bSuccess = false;
		return nullptr;
	}
	Topic2Kafka.Emplace(Topic, KafkaObj);
	//生成解析器
	UKafkaParserBase* Parser = Topic2KafkaParser.FindRef(Topic);
	if (!Parser->IsValidLowLevel())
	{
		if (KafkaSettingDA->IsValidLowLevel())
		{
			const FString* ParserType = Topic2Parser.Find(Topic);
			if (ParserType != nullptr)
			{
				auto ParserClass = KafkaSettingDA->Topic2KafkaParser.FindRef(*ParserType);
				if (ParserClass != nullptr)
				{
					Parser = NewObject<UKafkaParserBase>(this, ParserClass);
					Topic2KafkaParser.Add(Topic, Parser);
					Parser->ParserOverDelegate.BindUObject(this, &UKafkaSubsystem::ParserDataOver);
				}
			}
		}
	}
	KafkaObj->KafkaDataCBDelegate.BindUObject(Parser, &UKafkaParserBase::RecKafkaCBData);
	//多线程停止检查
	MultThreadCheckMap.Emplace(Topic, 0);
	//延迟订阅的UObject
	delaySubscribe(Topic);
	bSuccess = true;
	return KafkaObj;
}

bool UKafkaSubsystem::RemoveKafkaObject(FString Topic)
{
	UKafkaObject** Tmp = Topic2Kafka.Find(Topic);
	if (Tmp)
	{
		(*Tmp)->run = false;
		Topic2Kafka.Remove(Topic);
		return true;
	}
	return false;
}

bool UKafkaSubsystem::ShouldCreateSubsystem(UObject* Outer) const
{
	return true;
}

void UKafkaSubsystem::Initialize(FSubsystemCollectionBase& Collection)
{
	FString ItemPath = FPaths::ProjectPluginsDir() / TEXT("KafkaDataAsset'/DMHLKafka/Data/KafkaSetting.KafkaSetting'");
	KafkaSettingDA = LoadObject<UKafkaDataAsset>(this, *ItemPath);
	FString OutSettingPath = TEXT("Content/Config/KafkaConfig.json");
	if (KafkaSettingDA->IsValidLowLevel())
	{
		OutSettingPath = KafkaSettingDA->OutSettingPath;
	}
	//Kafka的Config文件默认路径
	if (!ImportKafkaConfig(OutSettingPath, KafkaConfigSetting))
	{
		UE_LOG(LogTemp, Log, TEXT("Parser Kafka file ERROR!!!"));
		return;
	}
	//项目开始只读第一个接口
	if (KafkaConfigSetting.Num() <= 0)
	{
		UE_LOG(LogTemp, Log, TEXT("KafkaConfigData Num is 0 !!!"));
		return;
	}
}

void UKafkaSubsystem::Deinitialize()
{
	UWorld* NowWorld = GetGameInstance()->GetWorld();
	if (NowWorld)
	{
		GetWorld()->GetTimerManager().ClearTimer(CheckConsumerTimer);
	}
	//清理所有线程
	for (auto& item : Topic2AsyncKafka)
	{
		//item.Value->EnsureCompletion();
		item.Value->Stop();
	}
	for (auto& item : Topic2KafkaThread)
	{
		item.Value->Kill(false);
	}
	for (auto& item : Topic2Kafka)
	{
		item.Value->run = false;
	}

}

bool UKafkaSubsystem::SubscribeTopics(UObject* Outer, const FString& InTopics, FString& ErrorData)
{
	if (!Topic2Kafka.Find(InTopics))
	{
		ErrorData = TEXT("KafkaConsumer is not valid!!!");
		return false;
	}
	if (!Topic2KafkaParser.Find(InTopics))
	{
		ErrorData = TEXT("Parser is not valid!!!");
		return false;
	}
	TWeakObjectPtr<UObject> OuterWeakPtr = Outer;
	FKafkaOuterObject* TmpArray = Topic2OuterObject.Find(InTopics);
	//firstRegisterOne
	if (!TmpArray)
	{
		FKafkaOuterObject Tmp;
		Tmp.Outers.Add(OuterWeakPtr);
		Topic2OuterObject.Add(InTopics, Tmp);
		return true;
	}
	TmpArray->Outers.Add(OuterWeakPtr);
	return true;
}

void UKafkaSubsystem::SubscribeTopicsBP(UObject* Outer, const FString& InTopics)
{
	if (!Topic2Kafka.Find(InTopics) || !Topic2KafkaParser.Find(InTopics))
	{
		TWeakObjectPtr<UObject> OuterWeakPtr = Outer;
		FKafkaOuterObject* TmpArray = Topic2PendingObject.Find(InTopics);
		//firstRegisterOne
		if (TmpArray == nullptr)
		{
			FKafkaOuterObject Tmp;
			Tmp.Outers.Add(OuterWeakPtr);
			Topic2OuterObject.Add(InTopics, Tmp);
			return;
		}
		TmpArray->Outers.Add(OuterWeakPtr);
		return;
	}
	FString ErrorData;
	SubscribeTopics(Outer, InTopics, ErrorData);
}

void UKafkaSubsystem::CheckAllActiveConsumer()
{
	for (const auto& item : Topic2Kafka)
	{
		//该消费者正在消费中
		if (!item.Value->run)
		{
			continue;
		}
		//该消费者消费计数器正在增加
		if (item.Value->ConsumerReadTime != MultThreadCheckMap[item.Value->TopicString])
		{
			MultThreadCheckMap.Emplace(item.Value->TopicString, item.Value->ConsumerReadTime);
			continue;
		}
		//异常情况重新启动线程拉数据
		FString Tmp;
		AsyncReadTopicAuto(item.Value->TopicString, Tmp, MultThreadSleepTime[item.Value->TopicString]);
	}
}

void UKafkaSubsystem::StartCheckConsumer(float inTime)
{
	UWorld* NowWorld = GetGameInstance()->GetWorld();
	if (NowWorld)
	{
		if (inTime < 1.f)
		{
			inTime = 1.f;
		}
		GetWorld()->GetTimerManager().SetTimer(CheckConsumerTimer, this, &UKafkaSubsystem::CheckAllActiveConsumer, inTime, true);
	}
}

void UKafkaSubsystem::ParserDataOver(UKafkaParserBase* InParser)
{
	const FString* OverTopic = Topic2KafkaParser.FindKey(InParser);
	if (OverTopic == nullptr)
	{
		return;
	}
	FKafkaOuterObject* TargetArray = Topic2OuterObject.Find(*OverTopic);
	if (TargetArray != nullptr)
	{
		for (const auto& elem : TargetArray->Outers)
		{
			UObject* ObjPtr = elem.Get();
			if (ObjPtr->IsValidLowLevel() && ObjPtr->GetClass()->ImplementsInterface(UKafkaInterface::StaticClass()))
			{
				IKafkaInterface* ReactingObject = nullptr;
				ReactingObject->Execute_RecKafkaParseredDataBP(ObjPtr, InParser);
			}
		}
	}

}

int32 UKafkaSubsystem::CheckConsumeTime(const FString& InTopic)
{
	const auto Tmp = Topic2Kafka.Find(InTopic);
	if (Tmp)
	{
		return (*Tmp)->ConsumerReadTime;
	}
	return -1;
}

void UKafkaSubsystem::StartAllTopic(const TArray<FString>& ExpTopics, TArray<FString>& CreateError, TArray<FString>& StartError)
{
	FString Err;
	float ComSleepTime = 0.2f;
	TMap<FString, FString> ComConfig;
	for (const auto& elem : KafkaConfigSetting)
	{
		FString&& TheTopic = elem.Topic.ToString();
		TMap<FString, FString> Config;
		if (TheTopic == TEXT("CommonConfig") || elem.Type == TEXT("Common"))
		{
			ComConfig = elem.ConfigSettings;
			ComSleepTime = elem.RefreshTimer;
			continue;
		}
		if (ExpTopics.Contains(TheTopic))
		{
			continue;
		}
		float SleepTime = UKismetMathLibrary::InRange_FloatFloat(
					elem.RefreshTimer, 0.f, 1.f, false, false) ? elem.RefreshTimer : ComSleepTime;
		Config.Append(ComConfig);
		Config.Append(elem.ConfigSettings);
		bool bCreateSuccess = false;
		MultThreadSleepTime.Emplace(TheTopic, SleepTime);
		CreateKafkaObject(Err, TheTopic, Config,bCreateSuccess);
		if (!bCreateSuccess)
		{
			CreateError.Add(TheTopic);
			continue;
		}
		if (!AsyncReadTopicAuto(TheTopic, Err, SleepTime))
		{
			StartError.Add(TheTopic);
			continue;
		}
		SuccessfulTopic.Add(TheTopic);
	}
}

void UKafkaSubsystem::StartAllTopicsByNewThreads(const TArray<FString>& ExpTopics, TArray<FString>& CreateError, TArray<FString>& StartError)
{
	FString Err;
	float ComSleepTime = 0.2f;
	TMap<FString, FString> ComConfig;
	for (const auto& elem : KafkaConfigSetting)
	{
		FString&& TheTopic = elem.Topic.ToString();
		TMap<FString, FString> Config;
		if (TheTopic == TEXT("CommonConfig") || elem.Type == TEXT("Common"))
		{
			ComConfig = elem.ConfigSettings;
			ComSleepTime = elem.RefreshTimer;
			continue;
		}
		if (ExpTopics.Contains(TheTopic))
		{
			continue;
		}
		float SleepTime = UKismetMathLibrary::InRange_FloatFloat(
			elem.RefreshTimer, 0.f, 1.f, false, false) ? elem.RefreshTimer : ComSleepTime;
		Config.Append(ComConfig);
		Config.Append(elem.ConfigSettings);
		bool bCreateSuccess = false;
		MultThreadSleepTime.Emplace(TheTopic, SleepTime);

		//生成解析器
		UKafkaParserBase* Parser = Topic2KafkaParser.FindRef(TheTopic);
		if (!Parser->IsValidLowLevel())
		{
			if (KafkaSettingDA->IsValidLowLevel())
			{
				const FString* ParserType = Topic2Parser.Find(TheTopic);
				if (ParserType != nullptr)
				{
					auto ParserClass = KafkaSettingDA->Topic2KafkaParser.FindRef(*ParserType);
					if (ParserClass != nullptr)
					{
						Parser = NewObject<UKafkaParserBase>(this, ParserClass);
						Topic2KafkaParser.Add(TheTopic, Parser);
						Parser->ParserOverDelegate.BindUObject(this, &UKafkaSubsystem::ParserDataOver);
					}
				}
			}
		}

		//找到KafkaObj并生成一个线程拉数据
		FKafkaAsyncObj* TmpThread = new FKafkaAsyncObj(Config, KafkaConfigSetting[0].ConfigSettings, TheTopic, SleepTime);
		TmpThread->DelegateOnKafkaRec.BindUObject(Parser, &UKafkaParserBase::RecKafkaCBData);
		Topic2AsyncKafka.Add(TheTopic, TmpThread);

		FRunnableThread* NewThread = FRunnableThread::Create(TmpThread, *TheTopic);

		Topic2KafkaThread.Add(TheTopic, NewThread);

		SuccessfulTopic.Add(TheTopic);
	}

}

void UKafkaSubsystem::StopAllTopic() 
{
	FString Err;
	TArray<FString> LastTopics;
	for (const auto& elem : SuccessfulTopic)
	{
		if (AsyncStopRead(elem, Err))
		{
			LastTopics.Add(elem);
		}
	}
	SuccessfulTopic = LastTopics;
}

void UKafkaSubsystem::delaySubscribe(const FString& InTopic)
{
	FString Err;
	FKafkaOuterObject* Objs = Topic2PendingObject.Find(InTopic);
	if (Objs)
	{
		for (auto Elem : Objs->Outers)
		{
			if (Elem.IsValid())
			{
				SubscribeTopics(Elem.Get(), InTopic, Err);
			}
		}
		Topic2PendingObject.Remove(InTopic);
	}
}

