#include <cstdlib>
#include<list>
#include<string>
#include<stdio.h>
#include<iostream>
#include<sstream>
#include<stdlib.h>
#include<fstream>
//#include"game.h"
#include<conio.h>
#define EMPTY *;
#define BLACK X;
#define WHITE O;
using namespace std;
std::ofstream output;
int max_count;
char player,max_player,min_player;
char square[8][8];
char board[8][8],boardfinal[8][8];
char opp_player;
int max_fit=-10,final_val=0,mov_i=-1,mov_j=-1;
int mov_check=-1;
int move[2][60];
int weight[8][8]={{99,-8,8,6,6,8,-8,99},{-8,-24,-4,-3,-3,-4,-24,-8},{8,-4,7,4,4,7,-4,8},{6,-3,4,0,0,4,-3,6},{6,-3,4,0,0,4,-3,6},{8,-4,7,4,4,7,-4,8},{-8,-24,-4,-3,-3,-4,-24,-8},{99,-8,8,6,6,8,-8,99}};
char output_board[8][8];
int strategy=-1;
int depth;
int flipper=0;
bool pass_turn=true;
std::ifstream input;
void setplayer(char Player)
{
player=Player;
}
void setopp_player(char player)
{
if(player=='X')
{
opp_player='O';
}
else
{
opp_player='X';
}
}
void setmove_zero()
{
for(int i=0;i<3;i++)
      {
       for(int j=0;j<60;j++)
       {
               move[i][j]=0;
       }
       }
} 
void direction_east(int i,int j)
	{
		if( ((i-1) <8) && ((j-1) < 8) && ((i-1) >= 0) && ((j-1) >= 0) )
		{
			if(board[i-1][j-1]==opp_player)
			{
				output_board[i-1][j-1] = player;
				flipper++;
				direction_east(i-1,j-1);
			}
			if (board[i-1][j-1]==player)
			{
				max_count = 1;
			}	
			if (board[i-1][j-1]=='*')
			{
				for(int s=0;s<flipper;s++)
				{
					output_board[i+s][j+s] = opp_player;
				}
			}
		}	
	}
 void direction_up(int i,int j)
	{
		if( ((i-1) <8) && ((i-1) >= 0) )
		{
			if(board[i-1][j]==opp_player)
			{
				output_board[i-1][j] =player;
				flipper++;
				direction_up(i-1,j);
			}
			if (board[i-1][j]==player)
			{
				max_count = 1;
			}	
			if (board[i-1][j]=='*')
			{
				for(int s=0;s<flipper;s++)
				{
					output_board[i+s][j] = opp_player;
				}
			}
		}	
	}
	
void direction_north(int i,int j)
	{
		if( ((i-1) <8) && ((j+1) <8) && ((i-1) >= 0) && ((j+1) >= 0) )
		{
			if(board[i-1][j]==opp_player)
			{
				output_board[i-1][j+1] =player;
				flipper++;
				direction_north(i-1,j+1);
			}
			if (board[i-1][j]==player)
			{
				max_count = 1;
			}
			if (board[i-1][j]=='*')
			{
				for(int s=0;s<flipper;s++)
				{
					output_board[i+s][j-s] = opp_player;
				}
			}
		}	
	}
 void direction_left(int i,int j)
	{
		if( ((j-1) <= 8) && ((j-1) >= 0) )
		{
			if(board[i][j-1]==opp_player)
			{
				output_board[i][j-1] = player;
				flipper++;
				direction_left(i,j-1);
			}
			if (board[i-1][j]==player)
			{
			max_count = 1;
			}
		if (board[i-1][j]=='*')
			{
				for(int s=0;s<flipper;s++)
				{
					output_board[i][j+s] = opp_player;
				}
			}
		}	
	}
 void direction_right(int i,int j)
	{
		if( ((j+1) < 8) && ((j+1) >= 0) )
		{
			if(board[i][j+1]==opp_player)
			{
				output_board[i][j+1] = player;
				flipper++;
				direction_right(i,j+1);
			}
			if (board[i][j+1]==player)
			{
			max_count = 1;
			}
		if (board[i][j+1]=='*')
			{
				for(int s=0;s<flipper;s++)
				{
					output_board[i][j-s] = opp_player;
				}
			}
		}	
	}
	void direction_south(int i,int j)
	{
		if( ((i+1) <8) && ((j-1) <8) && ((i+1) >= 0) && ((j-1) >= 0) )
		{
			if(board[i+1][j-1]==opp_player)
			{
				output_board[i+1][j-1] = player;
				flipper++;
				direction_south(i+1,j-1);
			}
			if (board[i+1][j-1]==player)
			{
			max_count = 1;
			}
			if (board[i+1][j-1]=='*')
			{
				for(int s=0;s<flipper;s++)
				{
					output_board[i-s][j+s] =opp_player;
				}
			}
		}	
	}
     void direction_down(int i,int j)
	{
		if( ((i+1) <8) && ((i+1) >= 0) )
		{
			if(board[i+1][j]==opp_player)
			{
				output_board[i+1][j] = player;
				flipper++;
				direction_down(i+1,j);
			}
			if (board[i+1][j]==player)
			{
			max_count = 1;
			}
			if (board[i+1][j]=='*')
			{
                for(int s=0;s<flipper;s++)
				{
				
					output_board[i-s][j] = opp_player;
				}
			}
		}	
	}
    void direction_west(int i,int j)
	{
		if( ((i+1) <8) && ((j+1) <8) && ((i+1) >= 0) && ((j+1) >= 0) )
		{
			if(board[i+1][j+1]==opp_player)
			{
				output_board[i+1][j+1] = player;
				flipper++;
				direction_west(i+1,j+1);
			}
			if (board[i+1][j+1]==player)
			{
			max_count = 1;
			}
			if (board[i+1][j+1]=='*')
			{
                for(int s=0;s<flipper;s++)
				{
				output_board[i-s][j-s] =opp_player;
				}
			}
		}	
	} 
void find_move(int i, int j)
{
    for(int l=0;l<8;l++)
    {
    for(int k=0;k<8;k++)
    {
      output_board[l][k]=board[l][k];
    }
    } 
    //cout<<"finding legal moves for i="<<i<<"and j="<<j;
    flipper = 0;
		if( (i-1 < 8) && (j-1 < 8) && (i-1 >= 0) && (j-1 >= 0) ) 
		{
			if(board[i-1][j-1]==opp_player)
			{
				output_board[i-1][j-1] =player;
				flipper++;
				direction_east(i-1,j-1);
				if(max_count == 1)
				{
					output_board[i][j] =player ;
					max_count = 0;
				}
			}
		}
		flipper = 0;
		if( ((i-1) <8) && ((i-1) >= 0) ) 
		{		
			if((board[i-1][j]==opp_player))
			{
				output_board[i-1][j] = player;
				flipper++;
				direction_up(i-1,j);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
						
			}
		}
		
		flipper = 0;
		if( ((i-1) < 8) && ((j+1) < 8) && ((i-1) >= 0) && ((j+1) >= 0) ) 
		{
			if(board[i-1][j+1]==opp_player)
			{		
				output_board[i-1][j+1] = player;
				flipper++;
				direction_north(i-1,j+1);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
			}
		}
				
		flipper = 0;
		if( ((j-1) <8) && ((j-1) >= 0) ) 
		{
			if(board[i][j-1]==opp_player)
			{		
				output_board[i][j-1] = player;
				flipper++;
				direction_left(i,j-1);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
			}
		}
				
		flipper = 0;
		if( ((j+1) < 8) && ((j+1) >= 0) ) 
		{
				
			if(board[i][j+1]==opp_player)
			{		
                output_board[i][j+1] = player;
				flipper++;
				direction_right(i,j+1);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
			}
		}
				
		flipper = 0;
		if( ((i+1) <8) && ((j-1) <8) && ((i+1) >= 0) && ((j-1) >= 0) ) 
		{
				
			if(board[i+1][j-1]==opp_player)
			{						
				output_board[i+1][j-1] = player;
				flipper++;
				direction_south(i+1,j-1);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
			}
		}
				
		flipper = 0;
		if( ((i+1) < 8) && ((j+1) >= 0) ) 
		{
					
			if(board[i+1][j]==opp_player)
			{			
				output_board[i+1][j] = player;
				flipper++;
				direction_down(i+1,j);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
			}
		}
				
		flipper = 0;
		if( ((i+1) <8) && ((j+1) <8) && ((i+1) >= 0) && ((j+1) >= 0) ) 
		{
			if(board[i+1][j+1]==opp_player)
			{			
				output_board[i+1][j+1] = player;
				flipper++;
				direction_west(i+1,j+1);
				if(max_count == 1)
				{
					output_board[i][j] = player;
					max_count = 0;
				}
			}
		}
                for(int l=0;l<8;l++)
                {
				   for(int k=0;k<8;k++)
					{
    	             if(output_board[l][k]!=board[l][k])
    	             {
							pass_turn=false;                   
                     }
                    }
                } 
                if(pass_turn==false)
                {
                  mov_check+=1;
                  char buf[10];
                  sprintf(buf,"%d%d",i,j);
                  move[0][mov_check]=atoi(buf);
                  //cout<<"found legal move for "<<buf<<endl;  
                  int player_weight=0,opp_player_weight=0; 
                  for(int l=0;l<8;l++)
		          {
			      for(int k=0;k<8;k++)
			      {
				  if(board[l][k]==player)
				  {
					player_weight += weight[l][k];
				  }
                  if(output_board[l][k]==opp_player)
				  {
					opp_player_weight += weight[l][k];
					
				  }
                  }
                  }
				move[1][mov_check]=player_weight-opp_player_weight;
				if(move[1][mov_check]>max_fit)
				{
                   max_fit=move[1][mov_check];
                   final_val=atoi(buf);
                   for(int l=0;l<8;l++)
					{
                     for(int m=0;m<8;m++)
					{
                     boardfinal[l][m]=output_board[l][m];
                     }
                     }
                     
                 }
                 if(move[1][mov_check]==max_fit)
                 {
                 if(atoi(buf)<final_val)
                 {
                 final_val=atoi(buf);
                 for(int l=0;l<8;l++)
					{
                     for(int m=0;m<8;m++)
					{
                     boardfinal[l][m]=output_board[l][m];
                     }
                     }
                     
                 
			}
		}               
       }           
                      
}
int greedy_strategy(char player, int depth)
{
    setopp_player(player);
               	for(int j=0;j<8;j++)
				{
					for(int k=0;k<8;k++)
					{
    	             if(board[j][k]=='*')
    	             {              
							find_move(j,k);                   
                     } 
                    }
                }
                for(int f=0;f<mov_check;f++)
                {
                   //cout<<move[0][f]<<"=>"<<move[1][f]<<endl;
                }
                mov_i=((final_val-(final_val%10))/10);
                mov_j=final_val%10;
                //cout<<"mov_i"<<mov_i<<"mov_j"<<mov_j<<endl;
                for(int j=0;j<8;j++)
				{
					for(int k=0;k<8;k++)
					{
							output<<boardfinal[j][k];                   
                     }
                     output<<endl; 
                    }
                  
                
//cout<<"greedy called"<<endl;
return 0;   
} 
//fix me:for return to check if a move can be made
int find_move_minimax(char max_player char min_player,int depth_count,char board_current[8][8])
{
    for(int j=0;j<8;j++)
				{
					for(int k=0;k<8;k++)
					{
    	             if(board[j][k]=='*')
    	             {              
							find_move(j,k);                   
                     } 
                    }
}
    

void recur_minimax(char max_player,char min_player,int depth_count,char board_current[8][8])
{
     if(depth_count>0)
{
if(depth_count%2==0)//even
{
player=max_player;

opp_player=min_player;                
depth_count-- ; 
cout<<"max player"<<endl;
recur_minimax(max_player,min_player,depth_count,board_current);
//call the find legal move function and print value;
//make the on the board_current for        
}
else//odd
{
player=min_player;
opp_player=min_player;   
depth_count--;
cout<<"min player"<<endl;
recur_minimax(max_player,min_player,depth_count,board_current);
}
}
else
{
    cout<<"fucked the recursion"<<endl;
}
}
void minimax_strategy(char max_player,char min_player, int depth)
{
     int depth_count=depth;
     recur_minimax(max_player,min_player,depth_count,board);
   /*mov_i=((final_val-(final_val%10))/10);
                mov_j=final_val%10;
                //cout<<"mov_i"<<mov_i<<"mov_j"<<mov_j<<endl;
                for(int j=0;j<8;j++)
				{
					for(int k=0;k<8;k++)
					{
							output<<boardfinal[j][k];                   
                     }
                     output<<endl; 
                    }
*/}

void fileread()
{
if (input.is_open())
 {
    input>>strategy>>player>>depth;
    cout<<strategy<<"\n";
    cout<<player<<"\n";
    cout<<depth<<"\n";
    for(int i=0;i<8;i++)
    {
    for(int j=0;j<8;j++)
     {
     input>>board[i][j];
     
    }
    }
}
}
int main(int argc, char *argv[])
{
 input.open("input.txt");
 output.open("output.txt");
 fileread();
 if(player=='X')
{
  max_player=player;
  min_player='O';
}
else
{
  max_player='O';
  min_player=player;
}    
if(strategy==2)
{
   int check=greedy_strategy(player,depth);
}
if(strategy==3)
{
minimax_strategy(max_player,min_player,depth);               
}
            
input.close();
output.close();
_getch();
}
