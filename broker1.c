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
#include<math.h>
#include<string.h>
#include <dirent.h>
#include <time.h>

#define MAX_SIZE 512
#define BROKER_NUM 1

struct message {
    long mtype;
    char mtext[MAX_SIZE];
};

struct con_message {
    long mtype;
    int socket_no;
};

struct publish
{
    int command;
    char topic[MAX_SIZE];
    char message[MAX_SIZE];
};


void generate_filename(char *p, char file_path[MAX_SIZE])
{
    int file_fd =0,i=0,len=0,x=0;
    ssize_t numRead,numWrite;
    file_fd = open(file_path, O_RDWR , S_IRUSR | S_IWUSR);
    if(file_fd < 0)
    {
        printf("Error opening a file\n");
        return; 
    }
    printf("YESS\n");
    char file_nm[MAX_SIZE];
    memset(file_nm, 0, MAX_SIZE * sizeof(char));
    numRead = read(file_fd,file_nm,MAX_SIZE);
    len = strlen(file_nm)-1;    //excluding \n
    for(i = len-1;i>=0;i--)
    {
        x = file_nm[i] - 48;
        if(x != 9)
        {
            x = x+1;
            file_nm[i] = x + 48;
            break;
        }
    }

    if(i == -1)
    {
        file_nm[0]='1';
        for(i=1;i<=len;i++)
        {
            file_nm[i] = '0';
        }
        file_nm[i]='\n';
    }
    else
    {
        for(i=i+1;i<len;i++)
        {
            file_nm[i]='0';
        }
        file_nm[i]='\n';
    }
    lseek(file_fd,0,0);
    numWrite = write(file_fd,file_nm,strlen(file_nm));
    printf("numWrite = %ld\n",numWrite);
    memset(file_nm, 0, MAX_SIZE * sizeof(char));
    lseek(file_fd,0,0);
    numRead = read(file_fd,file_nm,MAX_SIZE);
    strcpy(p,file_nm);
    printf("The next File name is %sXXX\n",file_nm);
}


int manage_folders(char *list_of_folders[100])
{
    int flag=0,i=0,j=0,k=0;
    struct dirent *de;
    char *file_list[100],*folder_list[100];
    char *fl;

    char cwrkd[1000];
    getcwd(cwrkd, 1000);
    //printf("Server's current working directory : %s\n", getcwd(cwrkd, 100));
    DIR *dr = opendir(cwrkd);
    if (dr == NULL)  // opendir returns NULL if couldn't open directory 
    { 
        printf("Could not open current directory" ); 
        return -1; 
    } 
    else
    {
        //printf("Server's current working directory : %s\n", getcwd(cwrkd, 100));
    }
    while ((de = readdir(dr)) != NULL) 
    {
        fl = de->d_name;
        i=0;
        flag=0;
        if(strlen(fl) == 1)
        {
            if(fl[0] !='.')
            {
                folder_list[j++] = fl;
            }
        }
        else if(strlen(fl) == 2)
        {
            if(fl[0] != '.' && fl[1] != '.')
            {
                folder_list[j++] = fl;
            }
        }
        else
        {
            for(i=0;i<strlen(fl);i++)
            {
                if(fl[i] == '.')
                {
                    flag = 1;
                }
            }
            if(flag == 1)
            {
                file_list[k++] = fl;
            }
            else
            {
                folder_list[j++] = fl;
            }
        }
        //printf("%s\n", de->d_name); 
    }

    /*printf("The files are\n");
    for(i=0;i<k;i++)
    {
        printf("%s\n",file_list[i]);
    }*/

    //printf("\n\nThe folders are\n");
    for(i=0;i<j;i++)
    {
        //printf("%s\n",folder_list[i]);
        list_of_folders[i] =folder_list[i];
    }
    closedir(dr);
    return j;
}

/*int get_topics()
{
    char cwrkd[MAX_SIZE];
    int no_of_args=0,i=0,j=0,k=0,fd=0;

    fd = open("topics.txt", O_RDWR , S_IRUSR | S_IWUSR);
    //lseek(fd)

    char *topics[100];
    char *a_ptr = strtok(buffer, "\n");
    strcat(old_topic,a_ptr);
    a_ptr = old_topic;
    while (a_ptr != NULL)
    {
        topics[no_of_args] = a_ptr;
        a_ptr = strtok(NULL, "\n");
        no_of_args++;
    }
    topics[no_of_args]=NULL;

    if(is_end == 1)
    {
        k = no_of_args;
        //printf("%sXXXX\n",topics[i]);
    }
    else
    {
        k = no_of_args -1;
        strcpy(new_topic,topics[no_of_args-1]);
    }

    for(i=0;i<k;i++)
    {
        //printf("%sXXXX\n",topics[i]);
        memset(id, 0, (MAX_SIZE+1) * sizeof(char));
        memset(cur_topic, 0, (MAX_SIZE+1) * sizeof(char));
        while((topics[i][j] != ' ') && ( j < strlen(topics[i])))
        {
            id[j] = topics[i][j];
            j++;
        }
        id[j]='\0';
        strcpy(cur_topic,topics[i]+j+1);
        printf("id = %sXXX\tcur_topic = %sXXX\n",id,cur_topic);
        if(strcmp(actual_topic,cur_topic) == 0)
        {
            sscanf(id, "%d", &topic_id);
            return topic_id;
        }
    }
    return -1;
}
*/



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
        return -1;
    }
    printf("Succesfull connected to the broker\n");
    return network_socket;
}

int server(int *broker_sock)
{
    char buffer[MAX_SIZE + 1];
    int fd;
    ssize_t numRead,numWrite;
    //char server_message[MAX_READ + 1];
    //char server_message[1024] = " You have reached the server\n";

    int server_socket;
    server_socket = socket(AF_INET,SOCK_STREAM,0);

    struct sockaddr_in server_address;
    server_address.sin_family= AF_INET;
    server_address.sin_port = htons(6001);
    if(inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr.s_addr) <= 0)
    {
        printf("error\n");
        exit(0);
    }
    //server_address.sin_addr.s_addr =htonl(655953580);        //INADDR_ANY;       //local_s1_addr.s_addr;
    //printf("server address is %d",server_address.sin_addr.s_addr);

    //printf("yes\nserver address is %d",local_s1_addr.s_addr);

    bind(server_socket, (struct sockaddr*) &server_address, sizeof(server_address));

    listen(server_socket, 5);

    int client_socket = 0;
    client_socket = accept(server_socket, NULL, NULL);
    printf("Succesfully connected with a left broker with socket %d\n",client_socket);
    //printf("Succesfully connected to client\n");
    
    *broker_sock = server_socket;
    return client_socket;
}

/*void create_server_route(int *sock1,int *left_broker, int *right_broker)
{
    int network_socket,server_socket,rg_broker=0,lt_broker=0,pid=0,status=0,is_child_status=0;

    //int msqid = msgget(IPC_PRIVATE, IPC_CREAT | 0600);

    rg_broker = socket(AF_INET,SOCK_STREAM,0);              //Making a client socket


    pid = fork();
    if(pid == 0)
    {
        int flag=0;
        flag = client(rg_broker);
        if(flag < 0)
        {
            if(flag == -1)
            {
                printf("Error converting IP address\n");
            }
        }
        exit(flag);
    }
    else
    {
        lt_broker = server(sock1);
        if(lt_broker == -1)
        {
            printf("Error having a connection with other server\n");
            exit(0);
        }
    }


    wait(&status);
    if(!WIFEXITED(status))
    {
        exit(0);
    }

    is_child_status= WEXITSTATUS(status);
    printf("status = %d\n",is_child_status);  //Check if any child exited with error
    if(is_child_status == 255)
    {
        printf("Error writing into message queue by the child\n");
        exit(0);    //Error in creating the directory
    }

    *left_broker = lt_broker;             //This server(sock1) acting as client to sock2
    *right_broker = rg_broker;                     //This server acting(sock1) as server to sock3
}
*/

int create_topic(char topic[MAX_SIZE])
{
    int flag=0,fd1=0,fd2=0,fd3=0;
    ssize_t numWrite=0,numRead=0;
    printf("Inside creating topic\n");
    char topic_id[MAX_SIZE];
    char file_path[MAX_SIZE];
    //int topic_id=0;

    if(chdir(topic) == 0)
    {
        printf("Already there is topic in this name\n");
        flag = -1;
        //return flag;
    }
    else
    {
        
        if (mkdir(topic, 0777) == -1)
        {
            printf("Error creating direcory %sXXX\n",topic);
            flag = -2;
            //return flag;
        }
        else
        {
            if(chdir(topic) != 0)
            {
                printf("Error changing directory to %sXXX\n",topic);
                flag = -1;
                return flag;
            }
            if(mkdir("FileName", 0777) == -1)
            {
                printf("Error creating direcory FileName \n");
                flag = -2;
                //return flag;
            }
            else
            {
                printf("FileName Folder created\n");
                fd1 = open("FileName/file_name.txt", O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
                if(fd1 < 0)
                {
                    printf("Error opening a file\n");
                    flag = -3;
                    //return flag;
                }
                else
                {
                    numWrite = write(fd1,"0",1);
                    if(numWrite <= 0)
                    {
                        printf("Error writing into a file\n");
                        flag = -4;
                        //return -4;
                    }
                }
            }
            if(mkdir("TopicId", 0777) == -1)
            {
                printf("Error creating direcory\n");
                flag = -2;
                //return flag;
            }
            else
            {

                fd2 = open("TopicId/min.txt", O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
                if(fd2 < 0)
                {
                    printf("Error opening a file\n");
                    flag = -3;
                    //return flag;
                }
                else
                {
                    numWrite = write(fd2,"0",1);
                    if(numWrite <= 0)
                    {
                        printf("Error writing into a file\n");
                        flag = -4;
                        //return flag;
                    }
                }
                fd3 = open("TopicId/max.txt", O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
                if(fd3 < 0)
                {
                    printf("Error opening a file\n");
                    flag = -3;
                    //return flag;
                }
                else
                {
                    numWrite = write(fd3,"0",1);
                    if(numWrite <= 0)
                    {
                        printf("Error writing into a file\n");
                        flag = -4;
                    }
                }
            }
            if(mkdir("CreationTime", 0777) == -1)
            {
                printf("Error creating direcory FileName \n");
                flag = -2;
                //return flag;
            }
            else
            {
                printf("CreationTime Folder created\n");
            }
            char topics_file[MAX_SIZE];
            int fd4=0;
            fd4 = open("../topics.txt", O_RDWR , S_IRUSR | S_IWUSR);
            if(fd4 < 0)
            {
                printf("Error opening topics file \n");
                flag = -3;
            }
            else
            {
                lseek(fd4,0,SEEK_END);
                numWrite = write(fd4,topic,strlen(topic));
                if(numWrite <= 0)
                {
                    printf("Error writing into topics file\n");
                    flag = -1;
                }
                else
                {
                    numWrite = write(fd4,"\n",1); 
                    if(numWrite <= 0)
                    {
                        printf("Error writing into topics file\n");
                        flag = -1;
                    }
                }

            }
        }
    }
    if(chdir("/home/sachin/Documents/Brokers/Broker1") != 0)
    {
        printf("Error while changing the directory\n");
        exit(0);
    }
    return flag;

}

int add_topicmessage(char topic[MAX_SIZE], char message[MAX_SIZE])
{
    int fd=0,fd2=0,fd3=0,fd4=0,flag=0,len=0;
    ssize_t numWrite=0,numRead=0;
    char fl_path[MAX_SIZE],max_fileno[MAX_SIZE];
    printf("Inside function\n");
    printf("The topic is %sXX\n",topic);
    printf("The message is %sXXX\n",message);

    char path[MAX_SIZE];
    char msgfile_name[MAX_SIZE];
    strcpy(path,topic);
    strcat(path,"/FileName/file_name.txt");
    printf("Path is %sXXX\n",path);
    generate_filename(msgfile_name,path);
    len = strlen(msgfile_name);
    msgfile_name[len-1]='\0';
    strcpy(max_fileno,msgfile_name);
    strcat(msgfile_name,".txt");
    printf("File name is %sXXX\n",msgfile_name);
    strcpy(fl_path,topic);
    strcat(fl_path,"/");
    strcat(fl_path,msgfile_name);
    fd = open(fl_path, O_RDWR | O_CREAT , S_IRUSR | S_IWUSR);
    if(fd < 0)
    {
        printf("Error creating the Topic Id File\n");
        //flag = -1;
        return -1;
    }
    printf("fd = %d\n",fd);
    printf("Message = %sXXX\n",message);
    numWrite = write(fd,message,strlen(message));
    if(numWrite <= 0)
    {
        printf("Error writng message into disk\n");
        return -2;
    }
    char maxid_path[MAX_SIZE],time_filepath[MAX_SIZE];
    strcpy(maxid_path,topic);
    strcat(maxid_path,"/TopicId/max.txt");
    printf("maxid_path = %sXXX\n",maxid_path);
    fd2 = open(maxid_path, O_RDWR | O_TRUNC , S_IRUSR | S_IWUSR);
    if(fd2 < 0)
    {
        printf("Error Updating Maximu File Id\n");
        //flag = -1;
        return -1;
    }
    numWrite = write(fd2,max_fileno,strlen(max_fileno));
    if(numWrite <= 0)
    {
        printf("Error writng message into disk\n");
        return -2;
    }
    printf("max file no = %sXXX\n",max_fileno);
    printf("%d\n",strcmp(max_fileno,"1"));
    if((strcmp(max_fileno,"1") == 0))
    {
        printf("YESS");
        char minid_path[MAX_SIZE];
        strcpy(minid_path,topic);
        strcat(minid_path,"/TopicId/min.txt");
        fd4 = open(minid_path, O_RDWR | O_TRUNC , S_IRUSR | S_IWUSR);
        if(fd4 < 0)
        {
            printf("Error Updating Maximu File Id\n");
            //flag = -1;
            return -1;
        }
        numWrite = write(fd4,max_fileno,strlen(max_fileno));
        if(numWrite <= 0)
        {
            printf("Error writng message into disk\n");
            return -2;
        }
    }

    strcpy(time_filepath,topic);
    strcat(time_filepath,"/CreationTime/");
    strcat(time_filepath,msgfile_name);
    fd3 = open(time_filepath, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if(fd3 < 0)
    {
        printf("Error Writing Creation Time of a message\n\n");
        //flag = -1;
        return -1;
    }
    //clock_t cur_time;
    //cur_time = clock();
    char c_time[MAX_SIZE];
    time_t cur_time; 
    cur_time = time(NULL);
    printf("current_time = %ld\n",cur_time);
    sprintf(c_time, "%ld", cur_time);
    printf("c_time = %sXXX\n",c_time);
    numWrite = write(fd3,c_time,strlen(c_time));
    if(numWrite <= 0)
    {
        printf("Error writng message into disk\n");
        return -2;
    }
    return 1;

}

int get_next_readablefile(char topic[MAX_SIZE],char file_id[MAX_SIZE])
{
    char min_path[MAX_SIZE],max_path[MAX_SIZE],buffer[MAX_SIZE];
    int min_fd=0,max_fd=0,min_fileno=0,max_fileno=0,min_msgno=0,old_fileno=0;
    ssize_t numRead=0,numWrite=0;
    strcpy(min_path,topic);
    strcpy(max_path,topic);
    strcat(min_path,"/TopicId/min.txt");
    strcat(max_path,"/TopicId/max.txt");
    min_fd = open(min_path, O_RDWR, S_IRUSR | S_IWUSR);
    if(min_fd < 0)
    {
        printf("Error opening a file\n");
        return -1;
    }
    else
    {
        numRead = read(min_fd,buffer,MAX_SIZE);
        if(numRead <= 0)
        {
            printf("Error reading a file\n");
        }
        else
        {
            buffer[numRead]='\0';
            printf("Min file number read = %sXXX\n",buffer);
            sscanf(buffer, "%d", &min_fileno); 
        }
    }
    memset(buffer, 0, (MAX_SIZE) * sizeof(char));
    max_fd = open(max_path, O_RDWR, S_IRUSR | S_IWUSR);
    if(max_fd < 0)
    {
        printf("Error opening a file\n");
    }
    else
    {
        numRead = read(max_fd,buffer,MAX_SIZE);
        if(numRead <= 0)
        {
            printf("Error reading a file\n");
        }
        else
        {
            buffer[numRead]='\0';
            printf("Max file number read = %s\n",buffer);
            sscanf(buffer, "%d", &max_fileno); 
        }

    }
    sscanf(file_id,"%d",&old_fileno);
    printf("Min file no = %d\nMAX file no =%d\nOld file No = %d\n",min_fileno,max_fileno,old_fileno);
    if(old_fileno >= max_fileno)
    {
        return 0;
    }
    else if(old_fileno < min_fileno)
    {
        return min_fileno;
    }
    else
    {
        return old_fileno+1;
    }

}
int contact_nextbroker(int right_broker, char topic[MAX_SIZE], char file_numbers[MAX_SIZE], char hop_count[MAX_SIZE],char response_msg[3][MAX_SIZE])
{
    int count=0;
    printf("Should Contact other brokers\n");
    ssize_t numRead=0,numWrite=0;
    char retreive_msg[5][MAX_SIZE];

    sscanf(hop_count, "%d", &count);
    if(count == 0)
    {
        return 0;
    }
    count = count-1;
    sprintf(retreive_msg[4], "%d", count);

    strcpy(retreive_msg[0],"subscriber");
    strcpy(retreive_msg[1],"1");
    strcpy(retreive_msg[2],topic);
    strcpy(retreive_msg[3],file_numbers);
    for(int i=0;i<5;i++)
    {
        printf("%sXXX\n",retreive_msg[i]);
    }
    numWrite = send(right_broker,retreive_msg,(5*MAX_SIZE),0);
    printf("NumWrite = %ld\n",numWrite);
    if(numWrite <= 0)
    {
        printf("Error writing to a broker\n");
        return -1;
    }
    numRead = recv(right_broker,response_msg,(3*MAX_SIZE),0);
    if(numRead <= 0)
    {
        printf("Error getting the message from the broker\n");
        return -1;
    }
    else
    {
        //message[numRead]='\0';
        printf("The message received is :\n%sXXX\n",response_msg[0]);
        printf("The updated file list = %sxxx\n",response_msg[1]);
    }
    return 1;
}


int get_message(char topic[MAX_SIZE], int next_fileno,char msg[MAX_SIZE])
{
    int fd=0;
    ssize_t numRead=0,numWrite=0;
    char f_name[MAX_SIZE],num[MAX_SIZE],f_path[MAX_SIZE];
        sprintf(num, "%d", next_fileno);
        strcpy(f_name,num);
        strcat(f_name,".txt");
        printf("The file name is %sXXX",f_name);
        strcpy(f_path,topic);
        strcat(f_path,"/");
        strcat(f_path,f_name);
        printf("The file path is %sXXX\n",f_path);
        fd = open(f_path, O_RDWR, S_IRUSR | S_IWUSR);
        if(fd < 0)
        {
            printf("Error opening a file\n");
            return -1;
        }
        else
        {
            lseek(fd,0,0);
            numRead = read(fd,msg,MAX_SIZE);
            if(numRead <= 0)
            {
                printf("Error reading a file\n");
                return -1;
            }
            else
            {
                msg[numRead]='\0';
                printf("The message to be sent is %sXXX\n",msg);
            }
        }
}

int retreive_one_message(int sockfd,char topic[MAX_SIZE], char file_nos[MAX_SIZE])
{
    printf("Toppic is = %sXXX\n",topic);
    printf("File_nos = %sXXX\n",file_nos);
    int no_of_args=0,i=0,next_fileno=0,fd=0;
    ssize_t numRead=0,numWrite=0;
    char file_no[MAX_SIZE],buffer[MAX_SIZE];
    char *file_ids[100];
    char *a_ptr = strtok(file_nos, " ");
    while (a_ptr != NULL)
    {
        file_ids[no_of_args] = a_ptr;
        a_ptr = strtok(NULL, " ");
        no_of_args++;
    }
    file_ids[no_of_args]=NULL;

    for(i=0;i<no_of_args;i++)
    {
        printf("%sXXX\n",file_ids[i]);
    }
    //strcpy(file_no,file_ids[0]);
    next_fileno = get_next_readablefile(topic,file_ids[BROKER_NUM-1]);
    return next_fileno;
    /*if(next_fileno < 0)
    {
        printf("Error getting next file id\n");
        return -1;
    }
    else if(next_fileno == 0)
    {
        printf("No Message available for this topic in this broker\n");
        contact_otherbrokers();
    }
    else
    {
        //send_filetosub(sockfd,topic,file_nos,next_fileno);
        get_message(topic);
    }
    return next_fileno;*/

}

int manage_deletion()
{
    int pid=0;
    pid = fork();
    if(pid == 0)
    {
        int j=1,flag=0;
        for(;;)
        {
            printf("%d\n",j);
            char *folder_list[100];
            char topicid_path[MAX_SIZE],minid_path[MAX_SIZE],maxid_path[MAX_SIZE],created_time_path[MAX_SIZE];
            char min_id[MAX_SIZE],max_id[MAX_SIZE],buffer[MAX_SIZE],created_time[MAX_SIZE];
            ssize_t numRead=0,numWrite=0;
            int num_of_folders=0,fd1=0,fd2=0,fd3=0;
            num_of_folders = manage_folders(folder_list);
            if(num_of_folders < 0)
            {
                printf("Error getting folders list\n");
                return -1;
            }
            int i=0;
            flag = 0;
            for(i=0;i<num_of_folders;i++)
            {
                printf("%sXXX\t",folder_list[i]);
                strcpy(minid_path,folder_list[i]);
                strcpy(maxid_path,folder_list[i]);
                strcpy(created_time_path,folder_list[i]);
                strcat(minid_path,"/TopicId/min.txt");
                strcat(maxid_path,"/TopicId/max.txt");
                strcat(created_time_path,"/CreationTime/");
                fd1 = open(minid_path, O_RDWR , S_IRUSR | S_IWUSR);
                if(fd1 < 0)
                {
                    printf("Error opening a file\n");
                    return -1; 
                }
                else
                {
                    lseek(fd1,0,0);
                    memset(buffer, 0, (MAX_SIZE) * sizeof(char));
                    numRead = read(fd1,buffer,MAX_SIZE);
                    if(numRead <= 0)
                    {
                        printf("Error reading minimum id file\n");
                        return -1;
                    }
                    else
                    {
                        buffer[numRead]='\0';
                        strcpy(min_id,buffer);
                        if(min_id[numRead-1] =='\n')
                        {
                            min_id[numRead-1]='\0';
                        }
                        printf("min_id = %sXXX\t",min_id);
                    }
                }
                fd2 = open(maxid_path, O_RDWR , S_IRUSR | S_IWUSR);
                if(fd2 < 0)
                {
                    printf("Error opening a file\n");
                    return -1; 
                }
                else
                {
                    lseek(fd2,0,0);
                    memset(buffer, 0, (MAX_SIZE) * sizeof(char));
                    numRead = read(fd2,buffer,MAX_SIZE);
                    if(numRead <= 0)
                    {
                        printf("Error reading minimum id file\n");
                        return -1;
                    }
                    else
                    {
                        buffer[numRead]='\0';
                        strcpy(max_id,buffer);
                        if(max_id[numRead-1] =='\n')
                        {
                            max_id[numRead-1]='\0';
                        }
                        printf("max_id = %sXXX\t",max_id);
                    }
                }
                if((strcmp(min_id,max_id) != 0))
                {
                    if((strcmp(max_id,"0") != 0))
                    {
                        strcat(created_time_path,min_id);
                        strcat(created_time_path,".txt");
                        printf("Created time path = %sXXX\n",created_time_path);
                        fd3 = open(created_time_path, O_RDWR , S_IRUSR | S_IWUSR);
                        if(fd3 < 0)
                        {
                            printf("Error opening a file\n");
                            return -1; 
                        }
                        else
                        {
                            lseek(fd3,0,0);
                            memset(buffer, 0, (MAX_SIZE) * sizeof(char));
                            numRead = read(fd3,buffer,MAX_SIZE);
                            if(numRead <= 0)
                            {
                                printf("Error reading minimum id file\n");
                                return -1;
                            }
                            else
                            {
                                buffer[numRead]='\0';
                                strcpy(created_time,buffer);
                                if(created_time[numRead-1] =='\n')
                                {
                                    created_time[numRead-1]='\0';
                                }
                                printf("created_time = %sXXX",created_time);
                                //char c_time[MAX_SIZE];
                                time_t crtd_tm,diff=0;
                                time_t cur_time; 
                                cur_time = time(NULL);
                                printf("current_time = %ld\n",cur_time);
                                //sprintf(c_time, "%ld", cur_time);
                                //printf("c_time = %sXXX\n",c_time);
                                sscanf(created_time, "%ld", &crtd_tm);
                                printf("created time = %ld\n",crtd_tm);
                                diff = cur_time - crtd_tm;
                                printf("diff = %ld\n",diff);
                                if(diff >= 60)
                                {
                                    flag = 1;
                                    char file_pth[MAX_SIZE];
                                    int fd5=0;
                                    if (remove(created_time_path) != 0)
                                    { 
                                      printf("Unable to delete created_time_path"); 
                                    }
                                    //remove(created_time_path);
                                    
                                    strcpy(file_pth,folder_list[i]);
                                    strcat(file_pth,"/");
                                    strcat(file_pth,min_id);
                                    strcat(file_pth,".txt");
                                    printf("file path = %sXXX\n",file_pth);

                                    if (remove(file_pth) != 0)
                                    { 
                                      printf("Unable to delete file_pth"); 
                                    }
                                    //remove(file_pth);

                                    fd5 = open(minid_path, O_RDWR , S_IRUSR | S_IWUSR);
                                    if(fd5 < 0)
                                    {
                                        printf("Error opening a file\n");
                                        return -1; 
                                    }
                                    else
                                    {
                                        int min_val=0;
                                        sscanf(min_id, "%d", &min_val);
                                        printf("min_Val = %d\n",min_val);
                                        min_val++;
                                        memset(min_id, 0, (MAX_SIZE) * sizeof(char));
                                        sprintf(min_id, "%d", min_val);
                                        printf("min_id = %sXX\n",min_id);
                                        numWrite = write(fd5,min_id,strlen(min_id));
                                        if(numWrite <= 0)
                                        {
                                            printf("Error writing to file\n");
                                            return -1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                printf("\n");
            }
            if(flag == 0)
            {
                sleep(1);
            }
            //sleep(5);
            j++;
            //printf("i = %d\n",i);
        }
        exit(0);
    }
}




int main(int argc, char* argv[])
{

    int server_socket=0,left_broker=0,right_broker=0,next_fileno=0;

    int fd=0, client_socket=0 , i=0 ,x=0, topic_id=0;
    int nfds=0 , maxi = 0 , nready=0 , sockfd=0 , maxfd=0;
    int client[FD_SETSIZE];
    fd_set readfds, writefds,allset;
    int p=0,q=0,r=0,s=0;
    char buffer[MAX_SIZE + 1] , server_message[MAX_SIZE + 1];
    struct sockaddr_in cliaddr;
    char pub_msg[5][MAX_SIZE];
    char message[MAX_SIZE];
    char reply_msg[3][MAX_SIZE];

    ssize_t numRead=0,numWrite=0,clilen=0;
    int rg_broker=0,rg_broker_portno=0;

    rg_broker_portno = 6002;
    char cwrkd[MAX_SIZE];
    if(chdir("/home/sachin/Documents/Brokers/Broker1") != 0)
    {
        printf("Error while changing the directory\n");
        exit(0);
    }
    printf("Server's current working directory : %s\n", getcwd(cwrkd, 100));

    i = manage_deletion();
    if(i < 0)
    {
        printf("Error managing deletions\n");
        exit(0);
    }

    int msqid = msgget(IPC_PRIVATE, IPC_CREAT | 0600);

    server_socket = socket(AF_INET,SOCK_STREAM,0);

    struct sockaddr_in server_address;
    server_address.sin_family= AF_INET;
    server_address.sin_port = htons(6001);
    if(inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr.s_addr) <= 0)
    {
        printf("error\n");
        exit(0);
    }
    bind(server_socket, (struct sockaddr*) &server_address, sizeof(server_address));
    listen(server_socket, 5);




    nfds = server_socket;
    maxi = -1;
    for(i=0;i<FD_SETSIZE ; i++)
    {
        client[i] = -1;
    }
    FD_ZERO(&allset);
    FD_SET(server_socket , &allset);
    FD_SET(right_broker , &allset);
    FD_SET(left_broker, &allset);
    maxfd = server_socket;
    for(;;)
    {
        readfds = allset;
        writefds = allset;
        nfds = maxfd + 1;
        nready = select(nfds , &readfds , &writefds, NULL, NULL);
        //printf("nready = %d\n",nready);
        clilen = sizeof(cliaddr);
        if(FD_ISSET(server_socket , &readfds))
        {
            client_socket = accept(server_socket, NULL, NULL);
            printf("A new client is connected\n");
            for(i=0;i<FD_SETSIZE;i++)
            {
                if(client[i] < 0)
                {
                    client[i] = client_socket;
                    break;
                }
            }
            if(i == FD_SETSIZE)
            {

                printf("Too many Clients\n");
                exit(0);
            }
            FD_SET(client_socket, &allset);
            if(client_socket > maxfd)
            {
                maxfd = client_socket;
            }
            if(i > maxi)
            {
                maxi = i;
            }
            if(--nready <= 0)
            {
                continue;
            }
        }

        for(i=0;i<=maxi;i++)
        {
            if((sockfd = client[i]) <0)
            {
                continue;
            }
            if(FD_ISSET(sockfd, &readfds))
            {
                memset(buffer, 0, (MAX_SIZE) * sizeof(char));
                numRead = recv(sockfd, pub_msg, (5*MAX_SIZE), 0);
                //numRead = read(sockfd,command_info,1000);
                //printf("The data received by client with socket id is %s\n",buffer);
                //printf("numraed = %d\n",numRead);
                if(numRead == 0)
                {
                    printf("Connection closed with client having connection id %d\n",sockfd);
                    close(sockfd);
                    FD_CLR(sockfd, &allset);
                    client[i] = -1;
                }
                else
                {
                    printf("Publisher/Subscriber %sXXX\n",pub_msg[0]);
                    printf("Command type id =  %sXXX\n",pub_msg[1]);
                    printf("topic received = %sXXX\n",pub_msg[2]);
                    printf("Message = %sXXX\n",pub_msg[3]);
                    printf("Hop Count = %sXXX\n",pub_msg[4]);
                    if(strcmp(pub_msg[0],"publisher") == 0)
                    {
                        char pub_response[MAX_SIZE];
                        if(pub_msg[1][0] == '1')
                        {
                            int x=0;
                            printf("Inside creating topic\n");
                            x = create_topic(pub_msg[2]);
                            //printf("x = %d\n",x);
                            if(x == -1)
                            {
                                strcpy(pub_response,"You have already created this topic\n");
                            }
                            else if(x < -1)
                            {
                                strcpy(pub_response,"Error creating the topic\n");
                            }
                            else
                            {
                                strcpy(pub_response,"Succesfully created the topic\n");
                            }
                            numWrite = write(sockfd,pub_response,strlen(pub_response));
                            if(numWrite <= 0)
                            {
                                printf("Error writing to publisher\n");
                            }
                        }
                        if(pub_msg[1][0] == '2')
                        {
                            int x=0;
                            printf("Inside Adding Message\n");
                            x = add_topicmessage(pub_msg[2],pub_msg[3]);
                            if(x < 0)
                            {
                                strcpy(pub_response,"Error writing the message\n");
                            }
                            else
                            {
                                strcpy(pub_response,"The message is written Succesfully\n");
                            }
                            numWrite = write(sockfd,pub_response,strlen(pub_response));
                            if(numWrite <= 0)
                            {
                                printf("Error writing to publisher\n");
                            }
                        }
                    }
                    else
                    {
                        if(pub_msg[1][0] == '1')
                        {
                            char updated_file_nos[MAX_SIZE];
                            char f_n[MAX_SIZE],f_ids[MAX_SIZE];
                            memset(message, 0, (MAX_SIZE) * sizeof(char));
                            printf("Retreive One message\n");
                            strcpy(f_ids,pub_msg[3]);
                            next_fileno = retreive_one_message(sockfd,pub_msg[2],f_ids);
                            /*if(next_fileno < 0)
                            {
                                printf("Error getting next file id\n");
                                strcpy(reply_msg[0],"0");
                                strcpy(reply_msg[1],"Error getting the message\n");
                                strcpy(reply_msg[2],"-1 -1 -1");
                                if(FD_ISSET(sockfd, &writefds))
                                {
                                    numWrite = send(sockfd,reply_msg,(3*MAX_SIZE),0);
                                    if(numWrite == 0)
                                    {
                                        printf("Error sending message to Subscriber\n");
                                    }
                                }
                                //return -1;
                            }*/
                            printf("next_fileno = %d\n",next_fileno);
                            if(next_fileno <= 0)
                            {
                                rg_broker = create_client("127.0.0.1",rg_broker_portno);
                                if(rg_broker < 0)
                                {
                                    printf("Error connecting to right broker\n");
                                }
                                //rg_broker = socket(AF_INET,SOCK_STREAM,0);
                                x = contact_nextbroker(rg_broker,pub_msg[2],pub_msg[3],pub_msg[4],reply_msg);
                                if(x == -1)
                                {
                                    memset(reply_msg, 0, (3*MAX_SIZE) * sizeof(char));
                                    strcpy(reply_msg[0],"0");
                                    strcpy(reply_msg[1],"Error getting the message\n");
                                    strcpy(reply_msg[2],"-1 -1 -1");
                                }
                                else if(x == 0)
                                {
                                    memset(reply_msg, 0, (3*MAX_SIZE) * sizeof(char));
                                    strcpy(reply_msg[0],"2");
                                    strcpy(reply_msg[1],"You are Up to Date. No messages to retreive\n");
                                    strcpy(reply_msg[2],"-1 -1 -1");
                                }
                                if(FD_ISSET(sockfd, &writefds))
                                {
                                    numWrite = send(sockfd,reply_msg,(3*MAX_SIZE),0);
                                    if(numWrite == 0)
                                    {
                                        printf("Error sending message to Subscriber\n");
                                    }
                                }
                                close(rg_broker);

                            }
                            else
                            {
                                x = get_message(pub_msg[2],next_fileno,message);
                                if(x == -1)
                                {
                                    printf("Error getting message\n");
                                    strcpy(reply_msg[0],"0");
                                    strcpy(reply_msg[1],"Error getting the message\n");
                                    strcpy(reply_msg[2],"-1 -1 -1");
                                }
                                else
                                {
                                    sprintf(f_n, "%d", next_fileno);
                                    strcpy(updated_file_nos,f_n);
                                    strcat(updated_file_nos," -1 -1");
                                    //printf("Updated file_nos = %sXXX\n",updated_file_nos);
                                    strcpy(reply_msg[0],"1");
                                    strcpy(reply_msg[1],message);
                                    strcpy(reply_msg[2],updated_file_nos);
                                    printf("Updated file_nos = %sXXX\n",reply_msg[2]);
                                    //numWrite = send(sockfd,reply_msg,(3*MAX_SIZE),0);

                                }
                                if(FD_ISSET(sockfd, &writefds))
                                {
                                    numWrite = send(sockfd,reply_msg,(3*MAX_SIZE),0);
                                    if(numWrite == 0)
                                    {
                                        printf("Error sending message to Subscriber\n");
                                    }
                                }
                            }
                        }
                        else if(pub_msg[1][0] =='2')
                        {
                            printf("Retreive all messages\n");
                        }
                    }
                }
                if(--nready <= 0)
                {
                    printf("YESS\n");
                    break;
                }
            }
        }
    }

    printf("Is id closing\n");
    close(server_socket);

}

