// Fill out your copyright notice in the Description page of Project Settings.

#pragma once

#include "CoreMinimal.h"
#include "Async/AsyncWork.h"



class UKafkaObject;
/**
 *
 */
//自动删除类型指针
//原始C++类使用UObject指针导致退出编辑器或Game可能会报错
class  FKafkaAutoAsyncTask : public FNonAbandonableTask
{
	friend class FAutoDeleteAsyncTask<FKafkaAutoAsyncTask>;

public:
	FKafkaAutoAsyncTask(UKafkaObject* Outobject, float OutTime = 1.f) :
		SleepTime(OutTime)
	{
		Outer = MakeWeakObjectPtr(Outobject);
	};
	FKafkaAutoAsyncTask() :
		Outer(nullptr)
	{};
	~FKafkaAutoAsyncTask() {};
	void DoWork();

private:
	//KafkaObject
	TWeakObjectPtr<UKafkaObject> Outer;
	//CbError
	FString StrError;
	//CbData
	FString StrData;
	//SleepTime
	float SleepTime = 0.f;
public:
	FORCEINLINE TStatId GetStatId() const
	{
		RETURN_QUICK_DECLARE_CYCLE_STAT(FKafkaAutoAsyncTask, STATGROUP_ThreadPoolAsyncTasks);
	}
};

//废弃,不要自己控制生命周期,使用上面那个自动的类,生命周期交给UE
class  FKafkaAsyncReadTask : public FNonAbandonableTask
{
	friend class FAsyncTask<FKafkaAsyncReadTask>;

public:
	FKafkaAsyncReadTask(UKafkaObject* Outobject, float OutTime = 1.f, bool bIsAutoFrequence = true) :
		SleepTime(OutTime),
		bAutoFrequence(bIsAutoFrequence)
	{
		Outer = MakeWeakObjectPtr(Outobject);
	};
	FKafkaAsyncReadTask() :
		Outer(nullptr)
	{};
	~FKafkaAsyncReadTask() {};
	void DoWork();

private:
	//KafkaObject
	TWeakObjectPtr<UKafkaObject> Outer;
	//CbError
	FString StrError;
	//未读取数据次数
	int32 NoDataTime = 0;
	//CbData
	FString StrData;
	//SleepTime
	float SleepTime = 0.f;
	//AutoFrequence
	bool bAutoFrequence;
public:
	FORCEINLINE TStatId GetStatId() const
	{
		RETURN_QUICK_DECLARE_CYCLE_STAT(FKafkaAsyncReadTask, STATGROUP_ThreadPoolAsyncTasks);
	}

};