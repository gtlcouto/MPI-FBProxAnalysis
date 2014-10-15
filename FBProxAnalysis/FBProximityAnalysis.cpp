
/*
        Author:                 Gustavo Couto
        Date:                   Nov. 23, 2013
        Description:			Determine shortest(line) distance between residences.dat and foodbanks.dat.
*/
#include <iostream>
#include <iomanip>
#include <fstream>
#include <mpi.h>
#include <cmath>
#include <vector>
#include <string>



using namespace std;


// structure to store x, y coordinates
typedef struct
{
        double x, y;
} Coord_t;


// structure to hold parsed display information
struct Range
{
        int zeroToOne, oneToTwo, twoToFive, overFive, recordsCount;
        double pZeroToOne, pOneToTwo, pTwoToFive, pOverFive;
        Range() : zeroToOne( 0 ), oneToTwo( 0 ), twoToFive( 0 ), overFive( 0 ), recordsCount( 0 ), pZeroToOne( 0 ), pOneToTwo( 0 ), pTwoToFive( 0 ), pOverFive( 0 )  { }
};

//Variables
const int resBytes = 24;
char* residence_fileName = "residences.dat";
char* foodbank_fileName = "foodbanks.dat";
double timer;
int addressCounter = 0;
Range ranges;
MPI_Datatype Range_Type;
vector<Coord_t> foodBankVector;


/*
 * Name: fileSize()
 * Input: const char* path
 * Output: streampos
 * Purpose: streampos with file size
 */
streampos fileSize( const char* filePath ){
       
    streampos fileSize = 0;
    ifstream file( filePath, ios::binary );
    fileSize = file.tellg();
    file.seekg( 0, ios::end );
    fileSize = file.tellg() - fileSize;
    file.close();

    return fileSize;
}


/*
 * Name: processData()
 * Input: None
 * Output: None
 * Purpose: each process receives a set of residences to process and send resulting data to the master
 */
void processData( int rank, int numProcs ) {
        try {
                ifstream in( residence_fileName );
                //file size
                streampos filesize = fileSize( residence_fileName );

                //determine the number of residences based on file size
                int nresidences = (int)filesize / resBytes;

                //set starting position
                int pos = rank;


                while( pos <=  nresidences ) {
                       
                        in.seekg( pos * resBytes );
                        addressCounter++;
                        double x, y;
                        in >> x >> y;

                        Range temp;
                        temp.zeroToOne = 0;


                        //determine distance to nearest foodbank
                        for( int i = 0; i < foodBankVector.size(); ++i ) {
                               
                                double fbDistance = std::sqrt(  ( ( x - foodBankVector[i].x ) * ( x - foodBankVector[i].x ) ) +
										( ( y - foodBankVector[i].y ) * ( y - foodBankVector[i].y ) ) );

                               
                                //convert to km
                                fbDistance /= 1000;


                                if( fbDistance <= 1.0 ) {
                                        temp.zeroToOne++;
                                        break;
                                } else if( fbDistance > 1.0 && fbDistance <= 2.0 ) {
                                        temp.oneToTwo++;
                                } else if( fbDistance > 2.0 && fbDistance <= 5.0 ) {
                                        temp.twoToFive++;
                                } else {
                                        temp.overFive++;
                                }              
                        }


                        //populate table with range
                        if( temp.zeroToOne > 0 )
                                ranges.zeroToOne++;
                        else if( temp.oneToTwo > 0 )
                                ranges.oneToTwo++;
                        else if( temp.twoToFive > 0 )
                                ranges.twoToFive++;
                        else
                                ranges.overFive++;


                        ranges.recordsCount++;


                        //increment position
                        pos += numProcs;
                }


                ranges.pZeroToOne = (static_cast<double>( ranges.zeroToOne ) / addressCounter) * 100;
                ranges.pOneToTwo = (static_cast<double>( ranges.oneToTwo )/ addressCounter) * 100;
                ranges.pTwoToFive = (static_cast<double>( ranges.twoToFive ) / addressCounter) * 100;
                ranges.pOverFive = (static_cast<double>( ranges.overFive ) / addressCounter) * 100;


                Range* rangesRecv;
                rangesRecv = ( Range* ) malloc( numProcs * sizeof( Range ) );
               
                //Master
                if (rank == 0) {
                        //determine the total time to process
                        timer = MPI_Wtime() - timer;
                        //gather info from the slaves
                        MPI_Gather( &ranges, 1, Range_Type, rangesRecv, 1, Range_Type, 0, MPI_COMM_WORLD);


                        int zeroToOneTotal = 0, oneToTwoTotal = 0, twoToFiveTotal = 0, overFiveTotal = 0, recordsCountTotal = 0;
                        double pZeroToOneTotal, pOneToTwoTotal, pTwoToFiveTotal, pOverFiveTotal;


                        //report
                        cout << "Proximity of Residential Addresses to Foodbanks in Toronto" << endl;
                        cout << "----------------------------------------------------------" << endl << endl;


                        cout << setw(26) << left << "Number of processes:" << numProcs << endl;
                        cout << setw(26) << left << "Elapsed Time in Seconds:" << timer << endl << endl;
                        //display information for each process
                        for( int i = 0; i < numProcs; ++i ) {


                                cout << "Process #" << i + 1 << " Results for " << rangesRecv[i].recordsCount << " Addresses..." << endl << endl;


                                cout << setw(20) << left << "Nearest Foodbank (km)" << setw(20) << right << "# of Addresses" << setw(20) << "% of Addresses" << endl;
                                cout << setw(20) << left << "---------------------" << setw(20) << right << "--------------" << setw(20) << "--------------" << endl;
                                cout << setw(20) << left << " 0 - 1" << setw(16) << right << rangesRecv[i].zeroToOne << setw(24) <<  rangesRecv[i].pZeroToOne << endl;
                                cout << setw(20) << left << " 1 - 2" << setw(16) << right << rangesRecv[i].oneToTwo << setw(24) << rangesRecv[i].pOneToTwo << endl;
                                cout << setw(20) << left << " 2 - 5" << setw(16) << right << rangesRecv[i].twoToFive << setw(24) << rangesRecv[i].pTwoToFive << endl;
                                cout << setw(20) << left << " > 5" << setw(16) << right << rangesRecv[i].overFive << setw(24) << rangesRecv[i].pOverFive << endl << endl;


                                zeroToOneTotal += rangesRecv[i].zeroToOne;
                                oneToTwoTotal += rangesRecv[i].oneToTwo;
                                twoToFiveTotal += rangesRecv[i].twoToFive;
                                overFiveTotal += rangesRecv[i].overFive;
                                recordsCountTotal += rangesRecv[i].recordsCount;
                        }
                        //get the total percentages
                        pZeroToOneTotal = (static_cast<double>( zeroToOneTotal ) / recordsCountTotal) * 100;
                        pOneToTwoTotal = (static_cast<double>( oneToTwoTotal )/ recordsCountTotal) * 100;
                        pTwoToFiveTotal = (static_cast<double>( twoToFiveTotal ) / recordsCountTotal) * 100;
                        pOverFiveTotal = (static_cast<double>( overFiveTotal ) / recordsCountTotal) * 100;
                        //display the combined results
                        cout << "Aggregate Results for all " << recordsCountTotal << " Addresses..." << endl << endl;


                        cout << setw(20) << left << "Nearest Foodbank (km)" << setw(20) << right << "# of Addresses" << setw(20) << "% of Addresses" << endl;
                        cout << setw(20) << left << "---------------------" << setw(20) << right << "--------------" << setw(20) << "--------------" << endl;
                        cout << setw(20) << left << " 0 - 1" << setw(16) << right << zeroToOneTotal << setw(24) <<  pZeroToOneTotal << endl;
                        cout << setw(20) << left << " 1 - 2" << setw(16) << right << oneToTwoTotal << setw(24) << pOneToTwoTotal << endl;
                        cout << setw(20) << left << " 2 - 5" << setw(16) << right << twoToFiveTotal << setw(24) << pTwoToFiveTotal << endl;
                        cout << setw(20) << left << " > 5" << setw(16) << right << overFiveTotal << setw(24) << pOverFiveTotal << endl << endl;
                } else { //gather information to the master
                        MPI_Gather( &ranges, 1, Range_Type, rangesRecv, 1, Range_Type, 0, MPI_COMM_WORLD);
                }
        } catch(std::exception ex) {
                std::cerr << ex.what() << std::endl;
        }
}


int main( int argc, char* argv[] ) {
       
        if( MPI_Init( &argc, &argv ) == MPI_SUCCESS ) {
                int numProcs, rank;
                MPI_Comm_size( MPI_COMM_WORLD, &numProcs );
                MPI_Comm_rank( MPI_COMM_WORLD, &rank );


                // Prepare input
                int block[] = {5, 4};
                MPI_Aint indices[2];
                indices[0] = 0;
                MPI_Type_extent(MPI_DOUBLE, &indices[1]);
                indices[1] *= 3;
                MPI_Datatype old[] = { MPI_INT, MPI_DOUBLE};


                //create database
                MPI_Type_struct(2, block, indices, old, &Range_Type);
                MPI_Type_commit(&Range_Type);

				//time to process all residences
                if (rank == 0 )
                {
                        timer = MPI_Wtime();
                }

                //input file                
                ifstream in( foodbank_fileName );


                if( !in ) {
                        cout << "Failed to open file: " << foodbank_fileName << endl;
                        return 1;
                }
                //populate vector with coordinates x y from file.
                while( !in.eof() ) {
                        Coord_t bank;
                        in >> bank.x >> bank.y;
                        foodBankVector.push_back( bank );
                }
               
                //start slaves
                processData( rank, numProcs );
                MPI_Type_free(&Range_Type);
                MPI_Finalize();
        }


        return 0;
}
