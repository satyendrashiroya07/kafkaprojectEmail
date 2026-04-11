package com.kafka.kafkaprojectEmail.io;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessdEventEntity, Long>{

	//ProcessdEventEntity save(ProcessdEventEntity processdEventEntity);
	
	ProcessdEventEntity findByMessageId(String messageId);

}
