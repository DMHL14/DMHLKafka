// Fill out your copyright notice in the Description page of Project Settings.


#include "KafkaObject.h"
#include "Misc/FileHelper.h"

bool UKafkaObject::CreateConsumer(FString& Err, const TMap<FString, FString>& ConfigurationSettings, const TMap<FString, FString>& GlobConfig)
{
	//0 配置
	std::string errorStr = "";
	topic_partition_isset = false;

	conf.Reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	tconf.Reset(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

	partition = RdKafka::Topic::PARTITION_UA;
	start_offset = RdKafka::Topic::OFFSET_BEGINNING;
	Err = "";
	for (auto& Elem : GlobConfig)
	{
		if (!SetConfigSetting(Elem, Err))
		{
			return false;
		}
	}
	for (auto& Elem : ConfigurationSettings)
	{
		if (!SetConfigSetting(Elem, Err))
		{
			return false;
		}
	}
	/* 确保以下配置成功
	** 1、连接
	** 2、配置消费者组ID
	*/
	conf->set("default_topic_conf", tconf.Get(), errorStr);
	if (errorStr.length() > 0)
	{
		Err = FString(errorStr.c_str());
		return false;
	}

	//1 创建消费者
	kafc.Reset(RdKafka::KafkaConsumer::create(conf.Get(), errorStr));
	if (errorStr.length() > 0)
	{
		Err = FString(errorStr.c_str());
		return false;
	}
	return true;
}

bool UKafkaObject::SubscribeTopic(FString topicName, FString& Err)
{
	topic_str = TCHAR_TO_UTF8(*topicName);
	std::vector<std::string> topics{ topic_str };
	RdKafka::ErrorCode ErrEnum = kafc->subscribe(topics);
	if (ErrEnum != RdKafka::ErrorCode::ERR_NO_ERROR)
	{
		Err = FString(RdKafka::err2str(ErrEnum).c_str());
		return false;
	}
	TopicString = topicName;
	topic_partition_isset = true;
	return true;
}

bool UKafkaObject::Consume(FString & Err, FString & Str, int waitTime /* = 200*/)
{
	++ConsumerReadTime;
	message.Reset(kafc->consume(waitTime));

	//if (!message.IsValid())
	//{
	//	//The Game is Over!!!
	//	return false;
	//}
	switch (message->err())
	{
	case RdKafka::ErrorCode::ERR__TIMED_OUT:
		Err = TopicString + TEXT("---NoMessages!!!---");
		break;
	case RdKafka::ErrorCode::ERR_NO_ERROR:
		//解决乱码
		FFileHelper::BufferToString(Str, static_cast<const uint8*>(message->payload()), message->len());
		Err = TEXT("");
		return true;
	default:
		run = false;
		Str = Err = TopicString + TEXT("---UNKNOWN_TOPIC!!!---");
		//引发崩溃的元凶
		//Err = FString(message->errstr().c_str());
		break;
	}
	Str = TEXT("");
	return false;
}



void UKafkaObject::Stop()
{
	if (kafc.IsValid())
	{
		kafc->close();
	}
	kafc.Reset();
}

bool UKafkaObject::SetConfigSetting(const TPair<FString, FString>& tpair, FString& Err)
{
	std::string key = std::string(TCHAR_TO_UTF8(*tpair.Key));
	std::string value = std::string(TCHAR_TO_UTF8(*tpair.Value));
	std::string errorStr = "";
	std::string t = key.substr(0, 6);
	RdKafka::Conf::ConfResult ConfResult = RdKafka::Conf::CONF_UNKNOWN;
	if (t == "topic.")
	{
		ConfResult = tconf->set(key, value, errorStr);
	}
	else
	{
		ConfResult = conf->set(key, value, errorStr);
	}
	switch (ConfResult)
	{
	case RdKafka::Conf::CONF_OK:
		lastErrorCode = 0;
		break;
	default:
		lastErrorCode = RdKafka::ErrorCode::ERR_INVALID_CONFIG;
		Err = FString(errorStr.c_str());
		return false;
	}
	return true;
}
