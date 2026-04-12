# 📧 Email Service

## 🚀 Overview

The **Email Service** is a microservice responsible for sending email notifications based on events received via **Apache Kafka**.

It listens to multiple event topics such as:

* 🧾 Order Created Event
* 👤 User Created Event

And sends email notifications accordingly.

---

## 🛠️ Tech Stack

* Java 17+
* Spring Boot
* Spring Kafka
* Spring Mail (JavaMailSender)
* PostgreSQL (optional logging)
* Lombok

---

## 📂 Project Structure

```plaintext
email-service
│── config
│── consumer
│── service
│── model / event
│── repository (optional)
│── util
```

---

## ⚙️ Configuration

### application.properties

```properties
server.port=8083

spring.application.name=kafkaprojectEmail

# Kafka Configuration
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=created-events
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Use ErrorHandlingDeserializer (IMPORTANT)
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer

# Trust your packages (IMPORTANT FIX)
spring.kafka.consumer.properties.spring.json.trusted.packages=shiroya.orderEvent,productEvent.userEvent

# Optional default type (if needed)
# spring.kafka.consumer.properties.spring.json.value.default.type=shiroya.orderEvent.OrderEvent

# Database (optional)
spring.datasource.url=jdbc:postgresql://localhost:5432/shiroyadb
spring.datasource.username=postgres
spring.datasource.password=shiroya

spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true

# Email Configuration (Gmail SMTP)
spring.mail.host=smtp.gmail.com
spring.mail.port=587
spring.mail.username=your-email@gmail.com
spring.mail.password=your-app-password

spring.mail.properties.mail.smtp.auth=true
spring.mail.properties.mail.smtp.starttls.enable=true
```

---

## 📩 Kafka Topics Consumed

| Topic Name    | Event Type |
| ------------- | ---------- |
| user-created  | UserEvent  |
| order-created | OrderEvent |

---

## 🧾 Event Models

### 1️⃣ UserEvent

```java
public class UserEvent {
    private String userName;
    private String userEmail;
}
```

---

### 2️⃣ OrderEvent

```java
public class OrderEvent {
    private String orderId;
    private String productId;
    private String userId;
    private Integer quantity;
    private String email;
    private String status;
}
```

---

## 📡 Kafka Consumer

```java
@KafkaListener(topics = "user-created", groupId = "created-events")
public void consumeUserEvent(UserEvent event) {
    emailService.sendUserCreatedEmail(event);
}

@KafkaListener(topics = "order-created", groupId = "created-events")
public void consumeOrderEvent(OrderEvent event) {
    emailService.sendOrderCreatedEmail(event);
}
```

---

## ✉️ Email Service Logic

```java
public void sendUserCreatedEmail(UserEvent event) {
    SimpleMailMessage message = new SimpleMailMessage();
    message.setTo(event.getUserEmail());
    message.setSubject("User Created Successfully");
    message.setText("Hello " + event.getUserName() + ", your account has been created!");

    mailSender.send(message);
}
```

---

## 🔄 Flow

```plaintext
User Service → Kafka → Email Service → Email Sent
Order Service → Kafka → Email Service → Email Sent
```

---

## ⚠️ Common Errors & Fixes

---

### ❌ Invalid Topic Exception

```
InvalidTopicException: User Created Successfully
```

✔️ Fix:

* Topic name should NOT contain spaces
* Use:

```
user-created
```

---

### ❌ Trusted Package Error

```
not in trusted packages
```

✔️ Fix:

```
spring.kafka.consumer.properties.spring.json.trusted.packages=shiroya.orderEvent,productEvent.userEvent
```

---

### ❌ Deserialization Error

```
RecordDeserializationException
```

✔️ Fix:
Use:

```
ErrorHandlingDeserializer
```

---

### ❌ Email Not Sending

✔️ Check:

* Gmail App Password (not normal password)
* SMTP enabled
* Internet connection

---

## 🔐 Security Note

❗ Never store plain password in code

Use:

* Environment Variables
* Spring Config Server
* Vault (Advanced)

Example:

```
spring.mail.password=${EMAIL_APP_PASSWORD}
```

---

## 🔮 Future Enhancements

* ✅ Retry mechanism (Kafka retry / DLQ)
* ✅ Email templates (HTML)
* ✅ Notification service (SMS, Push)
* ✅ Logging email history in DB
* ✅ Circuit breaker (Resilience4j)

---

## 👨‍💻 Author

**Satyendra Chaurasiya**

---

## ⭐ Notes

* Kafka must be running before starting service
* Topics should be pre-created or auto-created
* Use correct package names for events
* Avoid using `*` in production trusted packages
