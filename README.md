## Implementation of PageRank using Hadoop Map Reduce

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

### Extract Valid wikilinks and Generate Adjacency Graph

Given raw wiki XML dataset files with useful page informations contained inside \<page> tag, we have to extract page `title` and `outlinks` from this page. 
```xml
<page>
    <title>AccessibleComputing</title>
    <ns>0</ns>
    <id>10</id>
    <redirect title="Computer accessibility" />
    <revision>
      <id>631144794</id>
      <parentid>381202555</parentid>
      <timestamp>2014-10-26T04:50:23Z</timestamp>
      <contributor>
        <username>Paine Ellsworth</username>
        <id>9092818</id>
      </contributor>
      <comment>add [[WP:RCAT|rcat]]s</comment>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">#REDIRECT [[Computer accessibility]]
{{Redr|move|from CamelCase|up}}</text>
      <sha1>4ro7vvppa5kmm0o1egfjztzcwd0vabw</sha1>
    </revision>
  </page>
  ```
So first, we use `XMLInputFormat.class` to extract all the text between \<page> and \</page> tags.

Then we use `Dom4j` library to extract page `title` and `outlinks`. This step is implemented in the Mapper class.

Now Given a long page of page title followed by outlinks, we have to remove the redlinks which are the links that don't have page in our wiki dataset.
```
a   b c d e f g h
c   b d
...
```

to check whether the outlinks are redlink or not, we can pass the pagetitle to the outlinks 
```
title     outlinks
a         b c d e f g h
c         a b d
b         c
e         f h
h         e
      
        |
        | mapper
        v
        
<a,a>, <b,a>, <c,a>, <d,a>, <e,a>, <f,a>, <g,a>, <h,a>
<c,c>, <a,c>, <b,c>, <d,c>
<b,b>, <c,b>
<e,e>, <f,e>, <h,e>
<h,h>, <e,h>

        |
        | reducer
        v
        
outlinks   title   
<a,       (a,c)>
<b,       (a,b,c)>
<c,       (a,b,c)>
...
        |
        | output
        v
only output the key-value pair that the outlink that has correspondent pagetitle. 
```
Then use another `job` to reverse the outlink and page title. we got the adjacent graph with redlinks removed.
```
title     outlinks
a         b c e h
c         a b 
b         c
e         h
h         e
```


### Calculate pagerank

Given adjacent graph
```
title     outlinks
a         b c e h
c         a b 
b         c
e         h
h         e
```

First, set up a `job` to calculate the whole number `N` of pages 

Second, Initialize the iteration (Iteration 1) of updating page rank

```
title     outlinks
a         b c e h
c         a b 
b         c
e         h
h         e
      |
      | mapper
      v
      
<a, #b> <a, #c> <a, #e> <a, #h>
<c, #a> <c, #b>
<b, #c>
<e, #h>
<h, #e>
<b, c(a)>
<c, c(a)>
<e, c(a)>
<h, c(a)>
<a, c(c)>
<b, c(c)>
<c, c(b)>
<h, c(e)> 
<e, c(h)>                              

c(x) means the pageweight contribution from page x because x links to this page.

      |
      | reducer
      v
      
<a,        c(c) #b #c #e #h>
<b,        c(a) c(c) #c>
<c,        c(a) c(b) #a #b>
<e,        c(a) c(h) #h>
<h,        c(a) c(e) #e>

      |
      | output
      v
      
<a,        newpagerank, b, c, e, h>
<b,        newpagerank, c>
<c,        newpagerank, a,b>
<e,        newpagerank, h>
<h,        newpagerank, e>
```

Then all the following iteration process is similar to `Iteration 1`.

After several numbers of iteration, we got the output file 

```
<a,        pagerank, b, c, e, h>
<b,        pagerank, c>
<c,        pagerank, a,b>
<e,        pagerank, h>
<h,        pagerank, e>
```
We set up another `job` to extract the page `title` and `pagerank` and sort the page `title` in descending order of `pagerank` value.
