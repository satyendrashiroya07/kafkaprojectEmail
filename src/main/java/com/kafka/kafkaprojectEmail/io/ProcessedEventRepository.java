package com.kafka.kafkaprojectEmail.io;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessdEventEntity, Long>{
	
	ProcessdEventEntity findByMessageId(String messageId);

}
