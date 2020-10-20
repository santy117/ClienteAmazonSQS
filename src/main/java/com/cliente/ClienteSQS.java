package com.cliente;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Scanner;
// snippet-end:[sqs.java2.send_recieve_messages.import]


public class ClienteSQS {


    public static void main(String[] args) {

        Scanner entrada = new Scanner(System.in);
        long id;
        String concierto;
        String entradas;
        int eleccion=1;
        boolean finBusqueda=false;
        while(eleccion==1) {
            System.out.println("-----------------------\nSISTEMA DE COMPRA DE ENTRADAS \n ----------------------- \n");
            System.out.println("1. NUEVA COMPRA \n 2. SALIR \n");
            eleccion = Integer.parseInt(entrada.nextLine());
            if(eleccion!=1)
                System.exit(-1);
            System.out.print("Ingrese su id (solo numeros): ");
            id = Long.parseLong(entrada.nextLine());
            System.out.print("Seleccione concierto : \n 1. ACDC \n 2. The Strokes \n 3. Eminem \n");
            concierto = entrada.nextLine();
            switch (concierto) {
                case "1":
                    concierto = "ACDC";
                    break;
                case "2":
                    concierto = "The Strokes";
                    break;
                case "3":
                    concierto = "Eminem";
                    break;
                default:
                    System.out.println("Eleccion no valida. Vuelve a intentarlo");
            }
            System.out.println("Seleccione el numero de entradas: ");
            entradas = entrada.nextLine();

            SqsClient sqsClient = SqsClient.builder()
                    .region(Region.US_EAST_1)
                    .build();

            String queueName = "ColaTAInboxSantiago";
            String colaOutbox= "ColaTAOutboxSantiago";
            String message = "{\"idCompra\": " + id + ", \"concierto\": \"" + concierto + "\", \"tickets\": \"" + entradas + "\"}";

            sendMessage(sqsClient, queueName, message);
            System.out.println("Compra realizada. Esperando pdf de confirmación...");
            finBusqueda=false;
            while(!finBusqueda) {
                List<Message> mensajesRecibidos = receiveMessage(sqsClient, colaOutbox);

                for (Message m : mensajesRecibidos) {
                    //Cambiar visibilidad del mensaje a 0
                    GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                            .queueName(colaOutbox)
                            .build();
                    String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
                    ChangeMessageVisibilityRequest cmr = ChangeMessageVisibilityRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(m.receiptHandle())
                            .visibilityTimeout(0)
                            .build();
                    sqsClient.changeMessageVisibility(cmr);
                    if (m.body().contains(String.valueOf(id))) {
                        System.out.println(m.body());
                        System.out.println("Url recibida, eliminando mensaje de la cola...");
                        eliminaMessage(sqsClient, colaOutbox, m);
                        System.out.println("Mensaje eliminado");
                        finBusqueda=true;
                    }
                }
            }
        }
    }

    // snippet-start:[sqs.java2.send_recieve_messages.main]
    public static void eliminaMessage(SqsClient sqsClientOutbox, String queueName, Message m){
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqsClientOutbox.getQueueUrl(getQueueRequest).queueUrl();
        DeleteMessageRequest dmr = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build();
        sqsClientOutbox.deleteMessage(dmr);
    }
    public static List<Message> receiveMessage(SqsClient sqsClientOutbox, String queueName){
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqsClientOutbox.getQueueUrl(getQueueRequest).queueUrl();
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
       List<Message> message = sqsClientOutbox.receiveMessage(receiveRequest).messages();
       return message;


    }
    public static void sendMessage(SqsClient sqsClient, String queueName, String message) {

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse createResult = sqsClient.createQueue(request);

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .delaySeconds(5)
                    .build();
            sqsClient.sendMessage(sendMsgRequest);

        } catch (QueueNameExistsException e) {
            throw e;
        }
    }
}
// snippet-end:[sqs.java2.send_recieve_messages.main]