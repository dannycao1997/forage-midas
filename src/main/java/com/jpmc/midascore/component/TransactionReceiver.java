package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionReceiver {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void receive(Transaction transaction) {

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (isValidTransaction(sender, recipient, transaction.getAmount())) {
            processTransaction(sender, recipient, transaction.getAmount(), transaction.getIncentive());
        } else {

            System.out.println("Invalid transaction: " + transaction);
        }

    }

    private boolean isValidTransaction(UserRecord sender, UserRecord recipient, float amount) {

        return sender != null && recipient != null && sender.getBalance() >= amount;
    }

    private void processTransaction(UserRecord sender, UserRecord recipient, float amount, float incentive) {

        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount, incentive);
        transactionRepository.save(transactionRecord);

        System.out.println("Processed transaction: " + transactionRecord);


    }
}
