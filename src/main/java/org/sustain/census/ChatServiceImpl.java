package org.sustain.census;

import com.example.grpc.chat.Chat;
import com.example.grpc.chat.ChatServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashSet;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private static LinkedHashSet<StreamObserver<Chat.ChatMessageFromServer>> observers = new LinkedHashSet<>();

    @Override
    public StreamObserver<Chat.ChatMessage> chat(StreamObserver<Chat.ChatMessageFromServer> responseObserver) {
        return new StreamObserver<Chat.ChatMessage>() {
            @Override
            public void onNext(Chat.ChatMessage chatMessage) {
                // receive data from the client
                Chat.ChatMessageFromServer msg = Chat.ChatMessageFromServer.newBuilder()
                        .setMessage(chatMessage)
                        .build();
                observers
                        .forEach(o -> {
                            o.onNext(msg);
                        });
            }

            @Override
            public void onError(Throwable t) {
                observers.remove(responseObserver);
            }

            @Override
            public void onCompleted() {

            }
        };
    }
}
