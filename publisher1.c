#include<stdio.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include <string.h>
#include<sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/msg.h>
#include <mqueue.h>
#include<errno.h>
#include <sys/sem.h>
#include <sys/types.h>
#include <sys/ipc.h>

#include <time.h>
#include <poll.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/msg.h>
#include <sys/wait.h>


#define MAX_SIZE 512

struct publish
{
    int command;
    char topic[MAX_SIZE];
    char message[MAX_SIZE];
};



int create_client(char *ip , int port_num)
{
    int network_socket;

    network_socket = socket(AF_INET,SOCK_STREAM,0);

    struct sockaddr_in server_address;
    bzero(&server_address,sizeof(server_address));
    server_address.sin_family= AF_INET;
    server_address.sin_port = htons(port_num);
    server_address.sin_addr.s_addr = inet_addr("127.0.0.1");     // htonl(server_address.sin_addr.s_addr);

    int connection_status = connect(network_socket, (struct sockaddr*) &server_address, sizeof(server_address));
    if(connection_status == -1 )
    {
        printf("There was an error making a connection to the remote socket ");
        //return connection_status;
        exit(0);
    }
    return network_socket;
}



int main(int argc, char* argv[])
{
    int option=0,subscriber_socket=0,portnum=0,len=0;
    ssize_t numWrite=0,numRead=0;
    if(argc < 2)
    {
        printf("Please give the Details of the broker to which You want to connect to\n");
        exit(0);
    }
    sscanf(argv[1], "%d", &portnum);
    printf("Port num = %d\n",portnum);
    //portnum = 6001;
    subscriber_socket = create_client("127.0.0.1",portnum);
    printf("Enter 1 to create a topic\nEnter 2 to send a message\nEnter 3 to send a file\n");
    scanf("%d",&option);
    getchar();
    printf("Entered option is %d\n",option);
    /*numWrite = send(subscriber_socket,"Hello,Hi\n",9,0);
    if(numWrite == 0)
    {
        printf("Error writing to broker\n");
        exit(0);
    }*/
    
    if(option == 1)
    {
        char cr_topic[5][MAX_SIZE];
        char response[MAX_SIZE];
        strcpy(cr_topic[0],"publisher");
        strcpy(cr_topic[1],"1");
        char topic[MAX_SIZE];
        printf("Enter the Topic to be created\n");
        fgets(topic, MAX_SIZE, stdin);
        len = strlen(topic);
        topic[len-1]='\0';
        printf("The topic to be created is %sXXX\n",topic);
        strcpy(cr_topic[2],topic);
        cr_topic[3][0] = 0;
        cr_topic[4][0] = 0;
        cr_topic[5][0] = 0;
        numWrite = send(subscriber_socket,cr_topic,(5*MAX_SIZE),0);
        if(numWrite == 0)
        {
            printf("Error writing to broker\n");
            exit(0);
        }
        numRead = recv(subscriber_socket,response,MAX_SIZE,0);
        if(numRead <= 0)
        {

            printf("Error reading the response from Broker\n");
        }
        else
        {
            response[numRead]='\0';
            printf("%sXXX\n",response);
        }
    }
    if(option == 2)
    {
        char snd_msg[5][MAX_SIZE];
        char topic[MAX_SIZE],message[MAX_SIZE],response[MAX_SIZE];
        strcpy(snd_msg[0],"publisher");
        strcpy(snd_msg[1],"2");
        printf("Enter the topic on which you have to publish a message\n");
        fgets(topic, MAX_SIZE, stdin);
        len = strlen(topic);
        topic[len-1]='\0';
        strcpy(snd_msg[2],topic);
        printf("Enter the Message  to be published\n");
        fgets(message, MAX_SIZE, stdin);
        len = strlen(message);
        message[len-1]='\0';
        strcpy(snd_msg[3],message);
        printf("snd_msg[3] = %sXXX\n",snd_msg[3]);
        snd_msg[4][0] = 0;
        //snd_msg[5][0] = 0;
        numWrite = send(subscriber_socket,snd_msg,(5*MAX_SIZE),0);
        if(numWrite == 0)
        {

            printf("Error writing to broker\n");
            exit(0);
        }
        numRead = recv(subscriber_socket,response,MAX_SIZE,0);
        if(numRead == 0)
        {
            printf("Error writing to broker\n");
            close(subscriber_socket);
            exit(0);
        }
        else
        {
            response[numRead]='\0';
            printf("%sXXX",response);
            //numWrite = send(subscriber_socket,snd_msg,(6*MAX_SIZE),0);
        }        

    }
    if(option == 3)
    {
        int fd1=0;
        char snd_msg[5][MAX_SIZE];
        char topic[MAX_SIZE],message[MAX_SIZE],response[MAX_SIZE],file_path[MAX_SIZE];
        strcpy(snd_msg[0],"publisher");
        strcpy(snd_msg[1],"2");
        printf("Enter the topic on which you have to publish a message\n");
        fgets(topic, MAX_SIZE, stdin);
        len = strlen(topic);
        topic[len-1]='\0';
        strcpy(snd_msg[2],topic);
        printf("Enter the File to be published\n");
        fgets(file_path, MAX_SIZE, stdin);
        len = strlen(file_path);
        file_path[len-1]='\0';
        fd1 = open(file_path, O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
        if(fd1 < 0)
        {
            printf("Error opening the file\n");
            exit(0);
        }
        else
        {
            lseek(fd1,0,0);
            while((numRead = read(fd1,message,MAX_SIZE)) > 0)
            {
                strcpy(snd_msg[3],message);
                printf("snd_msg[3] = %sXXX\n",snd_msg[3]);
                snd_msg[4][0] = 0;
                //snd_msg[5][0] = 0;
                numWrite = send(subscriber_socket,snd_msg,(5*MAX_SIZE),0);
                if(numWrite == 0)
                {

                    printf("Error writing to broker\n");
                    close(subscriber_socket);
                    exit(0);
                }
                numRead = recv(subscriber_socket,response,MAX_SIZE,0);
                if(numRead == 0)
                {
                    printf("Error reading from broker\n");
                    close(subscriber_socket);
                    exit(0);
                }
                else
                {
                    response[numRead]='\0';
                    printf("%sXXX",response);
                    //numWrite = send(subscriber_socket,snd_msg,(6*MAX_SIZE),0);
                } 
                memset(message, 0, (MAX_SIZE) * sizeof(char)); 
            }
        }
          

    }
    close(subscriber_socket);

}


