#include "KafkaAsyncObj.h"
#include "Math/UnrealPlatformMathSSE.h"


uint32 FKafkaAsyncObj::Run()
{
	while (bIsRunning) {
		bRecSuccess = Consume(StrData, sleepTime * 1000.f);
		AsyncTask(ENamedThreads::GameThread, [=]() {
			//Obj->OnKafkaDataCB.Broadcast(bsuccess, StrError, StrData);
			DelegateOnKafkaRec.ExecuteIfBound(bRecSuccess, StrError, StrData);
			});
		FPlatformProcess::Sleep(sleepTime);
	}
	return 0;
}

bool FKafkaAsyncObj::Init()
{
	if (CreateConsumer(topicConfig, globConfig) &&
		SubscribeTopic()
		)
	{
		bIsRunning = true;
	}
	return bIsRunning;
}

void FKafkaAsyncObj::Exit()
{
	DelegateOnKafkaRec.Unbind();
	kafkaConsumer->close();
}

void FKafkaAsyncObj::Stop()
{
	bIsRunning = false;
}

bool FKafkaAsyncObj::SetConfigSetting(const TPair<FString, FString>& tpair, FString& Err)
{
	std::string key = std::string(TCHAR_TO_UTF8(*tpair.Key));
	std::string value = std::string(TCHAR_TO_UTF8(*tpair.Value));
	std::string errorStr = "";
	std::string t = key.substr(0, 6);
	RdKafka::Conf::ConfResult ConfResult = RdKafka::Conf::CONF_UNKNOWN;
	if (t == "topic.")
	{
		ConfResult = topicConf->set(key, value, errorStr);
	}
	else
	{
		ConfResult = globalConf->set(key, value, errorStr);
	}
	switch (ConfResult)
	{
	case RdKafka::Conf::CONF_OK:
		break;
	default:
		Err = FString(errorStr.c_str());
		return false;
	}
	return true;
}

bool FKafkaAsyncObj::Consume(FString& Str, int waitTime /*= 200*/)
{
	++ConsumerReadTime;
	message = kafkaConsumer->consume(waitTime);
	switch (message->err())
	{
	case RdKafka::ErrorCode::ERR__TIMED_OUT:
		break;
	case RdKafka::ErrorCode::ERR_NO_ERROR:
		//解决乱码
		FFileHelper::BufferToString(Str, static_cast<const uint8*>(message->payload()), message->len());
		return true;
	default:
		bIsRunning = false;
		Str = TopicString + TEXT("---UNKNOWN_TOPIC!!!---");
		//引发崩溃的元凶
		//Err = FString(message->errstr().c_str());
		break;
	}
	Str = TEXT("");
	return false;
}

bool FKafkaAsyncObj::CreateConsumer(const TMap<FString, FString>& ConfigurationSettings, const TMap<FString, FString>& GlobConfig)
{
	//0 配置
	std::string errorStr = "";

	globalConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	topicConf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	FString Err = "";
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
	globalConf->set("default_topic_conf", topicConf, errorStr);
	if (errorStr.length() > 0)
	{
		Err = FString(errorStr.c_str());
		return false;
	}

	//1 创建消费者
	kafkaConsumer = RdKafka::KafkaConsumer::create(globalConf, errorStr);
	if (errorStr.length() > 0)
	{
		Err = FString(errorStr.c_str());
		return false;
	}
	return true;
}

bool FKafkaAsyncObj::SubscribeTopic()
{
	FString Err;
	std::vector<std::string> topics{ TCHAR_TO_UTF8(*topicName) };
	RdKafka::ErrorCode ErrEnum = kafkaConsumer->subscribe(topics);
	if (ErrEnum != RdKafka::ErrorCode::ERR_NO_ERROR)
	{
		Err = FString(RdKafka::err2str(ErrEnum).c_str());
		return false;
	}
	TopicString = topicName;
	return true;
}
