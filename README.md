### Implementation of PageRank using Hadoop Map Reduce

>PageRank

One of the biggest changes in our lives in the decade was the availability of efficient and
accurate web search. Google is the first search engine which is able to defeat the spammers
who had made search almost useless. The technology innovation behind Google is called
PageRank. This project is to implement the PageRank to find the most important Wikipedia
pages on the provided the adjacency graph extracted from Wikipedia dataset using AWS Elastic
MapReduce(EMR).

>Definition of PageRank

PageRank is a function that assigns a real number to each page in the Web. The intent is that
the higher the PageRank of a page, the more important it is. The equation is as follows:

![N|Solid](https://raw.githubusercontent.com/mayborin/pagerank/master/page.png))

(1)
Where d = 0.85, p 1 ,p 2 ,...,p N are the pages under consideration, M(p i ) is the set of pages that link
to p i , L(p j ) is the number of outbound links on page p j , and N is the total number of pages. In our
project, we do not use teleport to deal with the sink node for simplicity. At the initial point,
each page is initialized with PageRank 1/N and the sum of the PageRank is 1. But the sum will
gradually decrease with the iterations due to the PageRank leaking in the sink nodes

> Input and output Definition

We will start with wikilinks raw data set which can be downloaded from [wikipedia database ](https://en.wikipedia.org/wiki/Wikipedia:Database_download). Sample input file can be found in this repository.

What we will get is page rank in descending order. We use Page Title to represent corresponding web page.

> Procedures

1. Extract Valid wikilinks and Generate Adjacency Graph
2. Calculate pagerank




