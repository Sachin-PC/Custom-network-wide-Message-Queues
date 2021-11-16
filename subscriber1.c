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
#define NUM_OF_BROKERS 3

struct publish
{
    int command;
    char topic[MAX_SIZE];
    char message[MAX_SIZE];
};

int register_topic(char topic[MAX_SIZE])
{
    int i=0,fd=0;
    char num[MAX_SIZE];
    ssize_t numWrite=0,numRead=0;
    char fl_path[MAX_SIZE];
    if(chdir(topic) != 0)
    {
        if (mkdir(topic, 0777) == -1)
        {
            printf("Error Subscribing to topic\n");
            exit(0);
        }
        printf("Succesfully subscribed to topic\n");
        for(i=1;i<=NUM_OF_BROKERS;i++)
        {
            memset(num, 0, (MAX_SIZE) * sizeof(char));
            memset(fl_path, 0, (MAX_SIZE) * sizeof(char));
            strcpy(fl_path,topic);
            strcat(fl_path,"/b");
            sprintf(num, "%d", i);
            printf("NUM = %sXXX\n",num); 
            strcat(fl_path,num);       //To convert int to ascii value
            strcat(fl_path,".txt");
            fd = open(fl_path, O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
            if(fd < 0)
            {
                printf("Error creating the Topic Id File\n");
                printf("Error creating file %sXXX\n",fl_path);
                exit(0);
            }
            numWrite = write(fd,"0",1);
            if(numWrite <= 0)
            {
                printf("Error writng message into disk\n");
                exit(0);
            }
        }
    }   
    else
    {
        printf("You are already subscribed to this topic\n");
    }
    if(chdir("/home/sachin/Documents/Subscribers/Subscriber1") != 0)
    {
        printf("Error while changing the directory\n");
        exit(0);
    }
}

void update_filenos(char topic[MAX_SIZE],char update_filenos[MAX_SIZE])
{
    //printf("Updating file numbers is %sXXX\n",update_filenos);

    //char cwrkd[MAX_SIZE];
    //printf("Subscriber's current working directory : %s\n", getcwd(cwrkd, MAX_SIZE));

    int no_of_args=0,i=0,fd=0;
    char *file_nos[100];
    char num[MAX_SIZE],fl_path[MAX_SIZE];
    ssize_t numWrite=0,numRead=0;
    char *a_ptr = strtok(update_filenos, " ");
    while (a_ptr != NULL)
    {
        file_nos[no_of_args] = a_ptr;
        a_ptr = strtok(NULL, " ");
        no_of_args++;
    }
    file_nos[no_of_args]=NULL;
    for(i=0;i<no_of_args;i++)
    {
        //printf("%sXXX\n",file_nos[i]);
        if(strcmp(file_nos[i],"-1") != 0)
        {
            memset(num, 0, (MAX_SIZE) * sizeof(char));
            memset(fl_path, 0, (MAX_SIZE) * sizeof(char));
            strcpy(fl_path,topic);
            strcat(fl_path,"/b");
            sprintf(num, "%d", i+1);
            //printf("NUM = %sXXX\n",num); 
            strcat(fl_path,num);       //To convert int to ascii value
            strcat(fl_path,".txt");
            fd = open(fl_path, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
            if(fd < 0)
            {
                printf("Error opening file %sXXX\n",fl_path);
                exit(0);
            }
            numWrite = write(fd,file_nos[i],strlen(file_nos[i]));
            if(numWrite <= 0)
            {
                printf("Error writng message into disk\n");
                exit(0);
            }   
        }
    }
    //printf("File Numbers updated Succesfully\n");
}

void get_next_message(int subscriber_socket,char topic[MAX_SIZE])
{
    char file_numbers[MAX_SIZE];
    char num[MAX_SIZE],fl_path[MAX_SIZE],buffer[MAX_SIZE],message[MAX_SIZE];
    char retreive_msg[5][MAX_SIZE];
    char response_msg[3][MAX_SIZE];
    int fd=0,i=0;
    ssize_t numRead=0,numWrite=0;
    if(chdir(topic) != 0)
    {
        printf("You are not registered to this topic\n");
        exit(0);
    }
    if(chdir("/home/sachin/Documents/Subscribers/Subscriber1") != 0)
    {
        printf("Error while changing the directory\n");
        exit(0);
    }
    memset(file_numbers, 0, (MAX_SIZE) * sizeof(char));
    for(i=1;i<=NUM_OF_BROKERS;i++)
    {
        memset(num, 0, (MAX_SIZE) * sizeof(char));
        memset(fl_path, 0, (MAX_SIZE) * sizeof(char));
        strcpy(fl_path,topic);
        strcat(fl_path,"/b");
        sprintf(num, "%d", i);
        strcat(fl_path,num);
        strcat(fl_path,".txt");
        fd = open(fl_path, O_RDWR , S_IRUSR | S_IWUSR);
        numRead = read(fd,buffer,MAX_SIZE);
        if(numRead < 0)
        {
            printf("Error reading the file\n");
            exit(0);
        }
        buffer[numRead]='\0';
        if(buffer[numRead-1] == '\n')
        {
            buffer[numRead-1] = '\0';
        }
        strcat(file_numbers,buffer);
        if(i != NUM_OF_BROKERS)
        {
            strcat(file_numbers," ");
        }
    }

    strcpy(retreive_msg[0],"subscriber");
    strcpy(retreive_msg[1],"1");
    strcpy(retreive_msg[2],topic);
    strcpy(retreive_msg[3],file_numbers);
    strcpy(retreive_msg[4],"2");
    //printf("file_nos = %sXXX\n",file_numbers);
    numWrite = send(subscriber_socket,retreive_msg,(5*MAX_SIZE),0);
    if(numWrite <= 0)
    {
        printf("Error sending a message to broker\n");
        exit(0);
    }
    memset(message, 0, (MAX_SIZE) * sizeof(char));
    numRead = recv(subscriber_socket,response_msg,(3*MAX_SIZE),0);
    if(numRead <= 0)
    {
        printf("Error getting the message from the broker\n");
        exit(0);
    }
    else
    {
        //message[numRead]='\0';
        if(strcmp(response_msg[0],"0") == 0)
        {
            printf("There was an error reading the message\n");
            exit(0);
        }
        else if(strcmp(response_msg[0],"2") == 0)
        {
            printf("The message received is :\n%sXXX\n",response_msg[1]);
            printf("The updated file list = %sxxx\n",response_msg[2]);
            update_filenos(topic,response_msg[2]);
            exit(0);
        }
        else
        {
            printf("The message received is :\n%sXXX\n",response_msg[1]);
            printf("The updated file list = %sxxx\n",response_msg[2]);
            update_filenos(topic,response_msg[2]);
        }
    }
}



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
    char cwrkd[MAX_SIZE];
    if(chdir("/home/sachin/Documents/Subscribers/Subscriber1") != 0)
    {
        printf("Error while changing the directory\n");
        exit(0);
    }
    //printf("Server's current working directory : %s\n", getcwd(cwrkd, 100));



    int option=0,subscriber_socket=0,portnum=0,len=0;
    ssize_t numWrite=0;
    char topic[MAX_SIZE];
    if(argc < 2)
    {
        printf("Please give the Details of the broker to which You want to connect to\n");
        exit(0);
    }
    sscanf(argv[1], "%d", &portnum);
    printf("Port num = %d\n",portnum);
    //char snd_msg[3][MAX_SIZE];
    //portnum = 6001;
    subscriber_socket = create_client("127.0.0.1",portnum);
    printf("Enter 1 to Subscribe to a topic\nEnter 2 to get next message of a particular topic\nEnter 3 to get all messages of a particular topic\n");
    scanf("%d",&option);
    getchar();

    if(option == 1)
    {
        printf("Enter the topic You want to subscribe to\n");
        fgets(topic, MAX_SIZE, stdin);
        len = strlen(topic);
        topic[len-1]='\0';
        printf("Entered topic is %s\n",topic);
        register_topic(topic);
    }
    if(option == 2)
    {
        printf("Enter the topic You want a message from\n");
        fgets(topic, MAX_SIZE, stdin);
        len = strlen(topic);
        topic[len-1]='\0';
        //printf("Entered topic is %s\n",topic);
        get_next_message(subscriber_socket,topic);
    }
    if(option == 3)
    {
        printf("Enter the topic You want a message from\n");
        fgets(topic, MAX_SIZE, stdin);
        len = strlen(topic);
        topic[len-1]='\0';
        for(;;)
        {
            get_next_message(subscriber_socket,topic);
        }

    }
}


