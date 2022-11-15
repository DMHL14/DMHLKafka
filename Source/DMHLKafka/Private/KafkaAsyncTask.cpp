#include "KafkaAsyncTask.h"
#include "KafkaObject.h"
#include "Math/UnrealPlatformMathSSE.h"

void FKafkaAsyncReadTask::DoWork()
{
	//循环读取数据
	GEngine->AddOnScreenDebugMessage(0, 0.5f, FColor::Green, TEXT("Kafka Thread Have Begun running!!!"));
	float TmpTime = SleepTime;
	while (Outer.IsValid()) {
		UKafkaObject* Obj = Outer.Get();
		if (!Obj->IsValidLowLevel() || Obj->IsPendingKill() 
			|| !Obj->run || StrError == TEXT("IllLegalTopic!!!"))
		{
			GEngine->AddOnScreenDebugMessage(0, 5.0f, FColor::Green, TEXT("Kafka Over!!!"));
			break;
		}
		bool bsuccess = Obj->Consume(StrError, StrData);
		AsyncTask(ENamedThreads::GameThread, [=]() {
			//Obj->OnKafkaDataCB.Broadcast(bsuccess, StrError, StrData);
			Obj->KafkaDataCBDelegate.ExecuteIfBound(bsuccess, StrError, StrData);
			});
		//动态频率——拉不到数据，就动态延长等待时间，一旦拉到就恢复默认查询时间
		//静态频率——拉不到数据也是固定的频率拉
		if (bAutoFrequence && !bsuccess && StrError == TEXT("TimeOut!!!"))
		{
			TmpTime = SleepTime + 2.f;
			++NoDataTime;
		}
		else
		{
			TmpTime = SleepTime;
		}
		FPlatformProcess::Sleep(TmpTime);
	}
}

void FKafkaAutoAsyncTask::DoWork()
{
	GEngine->AddOnScreenDebugMessage(0, 0.5f, FColor::Green, TEXT("Kafka Thread Have Begun running!!!"));
	while (Outer.IsValid()) {
		UKafkaObject* Obj = Outer.Get();
		if (Obj->IsPendingKill() || !Obj->run)
		{
			GEngine->AddOnScreenDebugMessage(0, 5.0f, FColor::Green, TEXT("Kafka Over!!!"));
			UE_LOG(LogTemp, Warning, TEXT("Kafka Over!!!"));
			break;
		}
		bool bsuccess = Obj->Consume(StrError, StrData, FMath::RoundToInt(SleepTime * 1000.f));
		AsyncTask(ENamedThreads::GameThread, [=]() {
			Obj->KafkaDataCBDelegate.ExecuteIfBound(bsuccess, StrError, StrData);
			});
		FPlatformProcess::Sleep(SleepTime);
	}
}
