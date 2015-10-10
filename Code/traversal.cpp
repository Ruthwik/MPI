  #include <stdio.h>
  #include <mpi.h>
  #include <stdlib.h>
  #include <string.h>
  #include <string>
  #include <sys/types.h>
  #include <dirent.h>
  #include <unistd.h>
  #include <errno.h>
  #include <vector>
  #include <map>
  #include <iostream>
  #include <fstream>
  #include <math.h>
  #include <queue>
  #include <time.h>
  #include "boost/filesystem.hpp"
  #define MAX_TABLE_WORD 100
  #define MAX_TABLE_COLS 100
  #define MASTER 0               /* taskid of first task */
  #define FROM_MASTER 1          /* setting a message type */
  #define FROM_WORKER 2          /* setting a message type */

  using namespace std;
  using namespace boost::filesystem;

  typedef std::pair<std::string,int> my_pair;

  bool sort_pred(const my_pair& left, const my_pair& right)
  {
    return left.second < right.second;
  }
  bool filter(char c)  
  {  
    return isalpha( c ) == 0;  
  } 

  void stopwords(vector<string> &v){         /* To get the stop words */

      static const char* fileName = "stopwords.txt";

          // Begin reading from file:
          ifstream fileStream(fileName);
         
          if (fileStream.is_open()){
              while (fileStream.good())
              {
                  // Store the next word in the file in a local variable.
                  string w;
                  fileStream >> w;
                  if (std::find(v.begin(), v.end(), w) == v.end()) 
                      v.push_back(w);
         
              }
          }
          else  
          {
              cerr << "Couldn't open the file." << endl;
              return;
          }
        
      
  }

  void tfidfcalculator(string filename,vector<string> &v1){  /*Calculates Term Frequency of each word in a file and returns top 10%*/
       vector<string> sw;
      // Will store the word and count.
      map<string, unsigned int> wordsCount;
      stopwords(sw);  //to get the stop words
          // Begin reading from file:
      ifstream fileStream(filename.c_str());

        if (fileStream.is_open())
        while (fileStream.good())
          {
                  // Store the next word in the file in a local variable.
            string w;
            fileStream >> w;
            w.resize( remove_if( w.begin(), w.end(), filter) - w.begin() ); 
            transform(w.begin(), w.end(), w.begin(), ::tolower);   //all words are transformed to lower case
            if (find(sw.begin(), sw.end(), w) == sw.end()){          //TF for only which are not stop words
                 if(wordsCount.find(w) == wordsCount.end())  // Then we've encountered the word for a first time.
                      wordsCount[w] = 1; // Initialize it to 1.
                 else 
                      wordsCount[w]++; // Just increment it.
                  }
          }
          else  
          {
              cerr << "Couldn't open the file." << endl;
              return;
          }
         int s=wordsCount.size();
         vector<pair<string, int> > mapcopy(wordsCount.begin(), wordsCount.end());
         sort(mapcopy.begin(), mapcopy.end(),sort_pred);
         int v_size=mapcopy.size();
         int top=(int)ceil(v_size*0.1);             //For top 10% of words
         for (int i = 0; i < top; ++i) {
            v1.push_back(mapcopy[i].first);
          }

        }


 int tfidf(string filename1,string filename2){   //calculates the intersection of words between two files
     
     vector<string> v1,v2,v3;

     tfidfcalculator(filename1,v1);
     tfidfcalculator(filename2,v2);

     int s1=v1.size();
     int s2=v2.size();
    // cout<<"s1 and s2 "<<s1<<" "<<s2<<"\n";
     std::set_intersection(  v1.begin(), v1.end(),v2.begin(), v2.end(),std::back_inserter( v3 )  );

     return (v3.size());   //returning the number of words in common between two files

  }




  main(int argc, char **argv) 
     {
       clock_t start, end;
       double total_time;
       start = clock();//time count starts 
       
       int root_process,d_no,str_size,mtype,ierr,my_id,num_procs,direct_or_not,sender; //d_no is the directory number
       std::vector<std::string> v_file,v_dir;
       MPI_Status status;
       double total_my_bcast_time = 0.0;
       vector<string> fullpath;
       std::vector<string> ::iterator it;
    
       //Setting up the environment, rank and size
       ierr = MPI_Init(&argc, &argv);
       ierr = MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
       ierr = MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
        
       root_process = 0;
       int count;

       total_my_bcast_time -= MPI_Wtime();
        
       if (num_procs < 2) {
       cout<<"Enter more than 1 process";
       MPI_Abort(MPI_COMM_WORLD, 0); 
       }

       if(my_id == 0) {
      
       //cout<<my_id<<" from master\n";
       mtype = FROM_MASTER;
       d_no=0;
       
       string log1 = "/home/mpiuser/Documents/files";
       int length = log1.length();
       char* path1 = (char *) malloc(length+1);
       strcpy(path1, log1.c_str());
       queue<string> q;
         
       DIR *dp;
       struct dirent *dirp;
       const char *in_dir=log1.c_str();

       if((dp = opendir(log1.c_str())) == NULL) {
          cout << "Error(" << errno << ") opening " << log1 << endl;
          return errno;
        }

       path p(path1);
       char* str1 = (char*)log1.c_str();
       for (auto i = directory_iterator(p); i != directory_iterator(); i++)
        {
            
          if (!is_directory(i->path()))         //If it is a file
          {        
               // v_file.push_back(i->path().filename().string());
           
          }
          else if(boost::filesystem::is_directory(p)){   //if it is a directory
            v_dir.push_back(i->path().filename().string());
           }
                    
        }
        for(it=v_dir.begin();it!=v_dir.end();it++){
           string a=*it;
           string filename=log1+"/"+a;
           q.push(filename);
         }
         /*for(it=v_file.begin();it!=v_file.end();it++){
             string a=*it;
             string filename=log1+"/"+a;
             q.push(filename);

         }*/
         string file_name;
         cout<<"\n****Enter the file name****\n";
         cin>>file_name;
         cout<<"\n";
    
         int l = file_name.length();
         char* temp = (char *) malloc(l+1);
         strcpy(temp, file_name.c_str());
         int n=1;
        while(n<num_procs){ 
        MPI_Send (&l, 2, MPI_INT,n, mtype, MPI_COMM_WORLD);
        MPI_Send (temp, l+1, MPI_CHAR,n, mtype, MPI_COMM_WORLD);
        n++;
      }
         int work_done=0;
         string a ;
         char* rec_buf; 
         int i=1;

      while(!q.empty()){
          
          i=1;
          int x=0;
          while(i<num_procs&&!q.empty()){
            a =q.front();
            int length = a.length();
            char* temp = (char *) malloc(length+1);
            strcpy(temp, a.c_str());
            string xy(temp);
            //cout<<"\n"<<xy<<"\n";
            q.pop();
              
            MPI_Send (&length, 2, MPI_INT,i, mtype, MPI_COMM_WORLD);
            MPI_Send (temp, length+1, MPI_CHAR, i, mtype, MPI_COMM_WORLD);   //sending the directory info
              
            i++;
            x++;
          }
           
          direct_or_not=1;    
          i=1;
           
          while(i<num_procs&&x>0){       //x is used for receiveing info of the same no of process that was sent the info
   
             MPI_Recv(&count, 2, MPI_INT, i, mtype, MPI_COMM_WORLD, &status);
            
             while(count>0){
                int length;
                MPI_Recv(&length, 2, MPI_INT, i, mtype, MPI_COMM_WORLD, &status);
                rec_buf = (char *) malloc(length+1);
                MPI_Recv(rec_buf, length+1, MPI_CHAR,i, mtype, MPI_COMM_WORLD, &status);
                string out(rec_buf,length);
                free(rec_buf);
                q.push(out);
                count--;

            }

          

            int sender = status.MPI_SOURCE;
            i++;
            x--;
      }
          if(!q.empty()){
            work_done=1;
            int s=q.size();
            int j=1;
             while(j<num_procs&&s!=0) {   
            MPI_Send (&work_done, 2,MPI_INT, j, mtype, MPI_COMM_WORLD);
             j++;
             s--;
            }
          }
              
          i=1;
          //cout<<"\nQUEUE SIZE:"<<q.size()<<"\n";
         } 
          work_done=0;
          for (int i = 1; i < num_procs; i++)
            MPI_Send (&work_done, 2,MPI_INT, i, mtype, MPI_COMM_WORLD);
        
         
         total_my_bcast_time += MPI_Wtime();
         //cout<<"\nTOTAL TIME TAKEN BY is "<<total_my_bcast_time<<"\n"; 
      
        
   
    }

 else{

       int l;
        char* rec_buf1;
        vector<string>::iterator it;
      mtype = FROM_MASTER;
      MPI_Recv(&l, 2, MPI_INT,MASTER, mtype, MPI_COMM_WORLD, &status);
      rec_buf1 = (char *) malloc(l+1);
      MPI_Recv(rec_buf1, l+1, MPI_CHAR,MASTER, mtype, MPI_COMM_WORLD, &status);
      string file_name(rec_buf1,l);
      free(rec_buf1);
   while(1==1){

      int times=1,length;
         
      std::vector<std::string> v_file,v_dir;
      char* rec_buf;
      mtype = FROM_MASTER;
      MPI_Recv(&length, 2, MPI_INT,MASTER, mtype, MPI_COMM_WORLD, &status);
      rec_buf = (char *) malloc(length+1);
      MPI_Recv(rec_buf, length+1, MPI_CHAR,MASTER, mtype, MPI_COMM_WORLD, &status);
      string dir(rec_buf,length);
      free(rec_buf);

      //cout<<dir<<"\n";  //to know which directory it received

      DIR *dp;
      struct dirent *dirp;

      if((dp = opendir(dir.c_str())) == NULL) {
      cout << "Error(" << errno << ") opening " << dir << endl;
      
      }

      path p(dir);
      char* str1 = (char*)dir.c_str();
      for (auto i = directory_iterator(p); i != directory_iterator(); i++)
      {

          if (!is_directory(i->path())) 
          {        
                v_file.push_back(i->path().filename().string());
           
          }
          else if(boost::filesystem::is_directory(p)){
          
             v_dir.push_back(i->path().filename().string());
            //cout<<"FRom slave "<<i->path().filename().string()<<"\n";

          }
              
      }
       
      vector<string> fullpath;
      std::vector<string> ::iterator it;
      int n=v_file.size();
      //cout<<"SIZE OF FILE VECTOR "<<n<<"\n";
      int mat[n][n];
        for(it=v_file.begin();it!=v_file.end();it++){
             string a=*it;
             string filename=dir+"/"+a;
             fullpath.push_back(filename);
           
         }
         // for(it=fullpath.begin();it!=fullpath.end();it++){
         //      cout<<*it<<"\n";

         // }

         for(int i=0;i<n;i++){
          for(int j=0;j<n;j++){
            if(i!=j&&i>j){
              mat[j][i]=mat[i][j]=tfidf(fullpath.at(i),fullpath.at(j));
              }
                  else{
                    mat[i][j]=1;
                  }
             }
          }
          
          cout<<"\n******Slave NO: "<<my_id<<"******\n";
          for(int i=0;i<n;i++){
          for(int j=0;j<n;j++){
           cout<<mat[i][j]<<" ";
          }
          cout<<"\n";
          }
           // cout<<"\n**coming*\n";
        // for(it=fullpath.begin();it!=fullpath.end();it++){
        //       cout<<*it<<"\n";

        //  }
       
      it=find(v_file.begin(),v_file.end(),file_name);
      if(it!=v_file.end()){
      cout<<"\n filename: "<<file_name<<"\n";
      auto pos = std::distance(v_file.begin(), it);
      cout<<"\n position: "<<pos<<"\n";
      //auto pos = it - vec.begin();
      cout<<"\n***Files Related to"<<file_name<<"***\n";
      for(int j=0;j<n;j++){
      if(j!=pos){
           if(mat[pos][j]!=0){
            cout<<v_file.at(j)<<"\n";
           }
         }
       }
      }
      int count=v_dir.size();

      MPI_Send(&count, 2, MPI_INT, 0, mtype, MPI_COMM_WORLD);  //sending the count for no of directories 

      for(it=v_dir.begin();it!=v_dir.end();it++){
        string a=*it;
        string filename=dir+"/"+a;
         
        int length = filename.length();
        char* temp = (char *) malloc(length+1);
        strcpy(temp, filename.c_str());
        
        MPI_Send (&length, 2, MPI_INT, 0, mtype, MPI_COMM_WORLD);
        MPI_Send (temp, length+1,MPI_CHAR, 0, mtype, MPI_COMM_WORLD);
               
       }

      int work_done;
      MPI_Recv(&work_done, 2, MPI_INT, 0, mtype, MPI_COMM_WORLD, &status);
      if(work_done==1){
         continue;
       }
      else{
       
          break;
       }
       closedir(dp);
     }
  }
       
   end = clock();//time count stops 
   total_time = ((double) (end - start)) / CLOCKS_PER_SEC;//calulate total time
   cout<<"Total time taken "<<total_time<<"\n";
  return 0;
  
  ierr = MPI_Finalize();

}




