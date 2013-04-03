#include "iostream"
#include "stdint.h"
#include "time.h"
#include "list"
#include "string"
#include "string.h"

#define FILE_TO_SORT "D:\\YandexTest2.dat"
#define MAX_MEMORY_USAGE_GB 8

#define MAX_ITEM_SIZE_BYTES sizeof(test)+100*1024*1024

using namespace std;

struct test {
    unsigned char	key[64];
    uint64_t		flags;
    uint64_t		crc;
    uint64_t		size;
};

class SortingItem 
{
public:
	SortingItem(test * lpInputStruct, unsigned char * lpinData):InputStruct(lpInputStruct),lpData(lpinData){}
	bool operator<(SortingItem& cmp){
		for(int i = 0; i < 64; i++)
			if(this->InputStruct->key[i] > cmp.InputStruct->key[i])
				return false;
			else if (this->InputStruct->key[i] < cmp.InputStruct->key[i])
				return true; 
		return false;
	}
	~SortingItem(){
		delete InputStruct;
		delete[] lpData;
	}
	test * InputStruct;
	unsigned char * lpData;
};

class SortedChunk : public list<SortingItem>
{
public:
	SortedChunk(string szinInputFileName, unsigned int dwinMaxMemoryUsagePerChunk):
		szInputFileName(szinInputFileName),
		dwMemoryAvailable(dwinMaxMemoryUsagePerChunk),
		dwMaxMemoryUsagePerChunk(dwinMaxMemoryUsagePerChunk),
		dwTotalChunks()
	{}
	bool flush()
	{
		if(this->empty())
			return false;
		bool res = false;
		this->sort();
		string szCurrentChunkFilename = szInputFileName + string("_chunk") + to_string(dwTotalChunks);
		FILE * hOutputFile = fopen(szCurrentChunkFilename.c_str(), "wb");	
		if(hOutputFile)
		{
			list<SortingItem>::iterator it;
			for(it = this->begin(); it != this->end(); it++)
			{
				fwrite((char *)it->InputStruct, sizeof(*it->InputStruct), 1, hOutputFile);
				fwrite((char *)it->lpData, it->InputStruct->size, 1, hOutputFile);
			}
			fclose(hOutputFile);
			dwTotalChunks++;
			res = true;
		}
		this->clear();
		dwMemoryAvailable = dwMaxMemoryUsagePerChunk;
		return res;
	}
	void addChunkItem(test * lpInputStruct, unsigned char * lpData)
	{
		if(dwMemoryAvailable < lpInputStruct->size + sizeof(*lpInputStruct))
			this->flush();
		test * lpNewInputStruct;
		unsigned char * lpNewData;
		do
		{
			try { 
				lpNewInputStruct = new test; 
				break;
			} catch (std::bad_alloc&) { 
				this->flush();
				cout << "No free memory available. Flushing buffer contents." << endl;
			}
		} while(true);
		memcpy(lpNewInputStruct, lpInputStruct, sizeof(*lpInputStruct));
		do
		{
			try { 
				lpNewData = new unsigned char[lpInputStruct->size]; 
				break;
			} catch (std::bad_alloc&) { 
				this->flush(); 
				cout << "No free memory available. Flushing buffer contents." << endl;
			}
		} while(true);
		memcpy(lpNewData, lpData, lpInputStruct->size);
		this->emplace_front(lpNewInputStruct, lpNewData);
		dwMemoryAvailable -= lpInputStruct->size + sizeof(*lpInputStruct);
	}
	uint64_t dwMemoryAvailable;
	unsigned int dwMaxMemoryUsagePerChunk;
	unsigned int dwTotalChunks;
	string szInputFileName;
};

bool iskey1Greaterkey2(char * key1, char * key2)
{
	for(int i = 0; i < 64; i++)
		if(key1[i] > key2[i])
			return true;
		else if (key1[i] < key2[i])
			return false; 
	return false;
}

class ChunkMerger
{
public:
	ChunkMerger(string szInputFileName, unsigned int dwChunkId):eofReached(false),chunkFile()
	{
		szChunkFilename = szInputFileName + string("_chunk") + to_string(dwChunkId);
	}
	bool Init()
	{
		chunkFile = fopen(szChunkFilename.c_str(), "rb");
		if(chunkFile)
		{
			int readed = fread((char *)&InputStruct, sizeof(InputStruct), 1, chunkFile);
			if(readed)
				return true;
		}
		return false;
	}
	unsigned char * key()
	{
		if(eofReached)
			return NULL;
		return &InputStruct.key[0];
	}
	bool next()
	{
		if(eofReached)
			return NULL;
		if(fseek(chunkFile, InputStruct.size, SEEK_CUR))
		{
			eofReached = true;
			return false;
		}
		int readed = fread((char *)&InputStruct, sizeof(InputStruct), 1, chunkFile);
		if(readed)
			return true;
		eofReached = true;
		return false;
	}
	uint64_t copyItemToBufferAndGoNext(char * lpBuf)
	{
		if(eofReached)
			return 0;
		memcpy(lpBuf, &InputStruct, sizeof(InputStruct));
		if(!chunkFile)
			return 0;
		int readed = fread((char *)&lpBuf[sizeof(InputStruct)], InputStruct.size, 1, chunkFile);
		if(readed)
		{
			uint64_t res = sizeof(InputStruct)+InputStruct.size;	
			readed = fread((char *)&InputStruct, sizeof(InputStruct), 1, chunkFile);
			if(!readed)
				eofReached = true;
			return res;
		}
		return 0;
	}
	~ChunkMerger()
	{
		if(chunkFile)
		{
			fclose(chunkFile);
			remove(szChunkFilename.c_str());
		}
	}
private:
	bool eofReached;
	string szChunkFilename;
	FILE * chunkFile;
	test InputStruct;
};

void ExternalSorting3(const char * szInputFileName)
{
	FILE * hInputFile;
	hInputFile = fopen(szInputFileName, "rb");
	if(hInputFile)
	{

		uint64_t dwMaxMemoryUsage = MAX_MEMORY_USAGE_GB;
		dwMaxMemoryUsage = dwMaxMemoryUsage*1024*1024*1024 - MAX_ITEM_SIZE_BYTES;

		SortedChunk * stSortedChunk = new SortedChunk(string(szInputFileName), dwMaxMemoryUsage);
		
		test * InputStruct = new test;
		unsigned char * lpData = new unsigned char [MAX_ITEM_SIZE_BYTES];
		uint64_t qwCurrentOffset = 0;
		uint64_t readed;
		while(!feof(hInputFile))
		{
			readed = fread((char *)InputStruct, sizeof(*InputStruct), 1, hInputFile);
			if(readed)
			{
				readed = fread((char *)lpData, InputStruct->size, 1, hInputFile);
				if(readed)
				{
					stSortedChunk->addChunkItem(InputStruct, lpData);
				}
				qwCurrentOffset += sizeof(InputStruct) + InputStruct->size;
				cout << (qwCurrentOffset / (1024*1024)) << " Mb processed\r";
			}
		}
		delete InputStruct;
		delete[] lpData;

		fclose(hInputFile);
		
		stSortedChunk->flush();
	
		int dwTotalChunks = stSortedChunk->dwTotalChunks;
		
		delete stSortedChunk;

		list<ChunkMerger> * chunkMergersList = new list<ChunkMerger>;

		for(int i = 0; i < dwTotalChunks; i++)
		{
			ChunkMerger * tmp = new ChunkMerger(szInputFileName, i);
			if(tmp->Init())
				chunkMergersList->push_back(*tmp);
		}

		if(!chunkMergersList->empty())
		{

			string szOutputFileName = szInputFileName + string(".Sorted");
			FILE * hOutputFile = fopen(szOutputFileName.c_str(), "wb");	

			if(hOutputFile)
			{
				char * lpOutputBuf = new char[MAX_ITEM_SIZE_BYTES];

				list<ChunkMerger>::iterator current;
				list<ChunkMerger>::iterator smallest;
				while(chunkMergersList->size() > 1)
				{
					current = chunkMergersList->begin();
					smallest = current++;
					char * smallestKey = (char *)smallest->key();
					if(!smallestKey)
					{
						chunkMergersList->erase(smallest);
						continue;
					}
					while(current != chunkMergersList->end())
					{
						char * curKey = (char *)current->key();
						if(!curKey)
						{
							current = chunkMergersList->erase(current);
							continue;
						}
						if(iskey1Greaterkey2(smallestKey, curKey))
						{
							smallest = current;
							smallestKey = curKey;
						}
						current++;
					}
					uint64_t itemSize = smallest->copyItemToBufferAndGoNext(lpOutputBuf);
					fwrite(lpOutputBuf, itemSize, 1, hOutputFile);
				}			

				if(chunkMergersList->size() == 1)
				{
					current = chunkMergersList->begin();
					uint64_t itemSize;
					while(itemSize = current->copyItemToBufferAndGoNext(lpOutputBuf))
						fwrite(lpOutputBuf, itemSize, 1, hOutputFile);
				}
				
				delete[] lpOutputBuf;

				fclose(hOutputFile);
			}
		}

		delete chunkMergersList;
	}
}

int main() 
{
	clock_t start = clock();
	ExternalSorting3(FILE_TO_SORT);
	clock_t end = clock();
	cout <<  "Elapsed: " << ((double)(end - start) / CLOCKS_PER_SEC) << " seconds" << endl;
	return 0;
}