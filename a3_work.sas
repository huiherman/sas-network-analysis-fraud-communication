/* Q2 - How many records in the unique dataset?*/

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Count distinct roles */
proc sql;
    /* Step 1: Create a new table that only keeps unique rows from the original table */
    create table temp_unique_rows as
    select distinct *
    from A3IMPORT;

    /* Step 2: Count the number of unique rows in the new table */
    select count(*) as unique_count
    from temp_unique_rows;
quit;

/* Q3 - How many nodes are there on the network? */

/* Count the number of unique node in the A3IMPORT table */

/* Step 1: Create a new dataset that contains all nodes from both columns */
data all_nodes;
    set A3IMPORT(keep=from to);  /* Use only the from and to columns */
    node = from; output;   /* Save from as a node */
    node = to; output;     /* Save to as a node */
run;

/* Step 2: Remove any duplicate nodes so we only count each node once */
proc sort data=all_nodes nodupkey;
    by node;   /* Sort by node and keep only unique ones */
run;

/* Step 3: Count the total number of unique nodes */
proc sql;
    select count(distinct node) as NumNodes   /* Count how many unique nodes there are */
    from all_nodes;
quit;

/*Q4 - How many links are on the network? */

/* Step 1: Remove any duplicate links from the dataset */
proc sort data=A3IMPORT nodupkey out=unique_links;
    by from to;   /* Look at the from and to columns to find duplicates */
run;

/* Step 2: Count how many unique links exist in the network */
proc sql;
    select count(*) as NumLinks   /* Count the number of rows, each row is one unique link */
    from unique_links;            /* Use the cleaned dataset with no duplicate links */
quit;

/* Q5 - How many articulation points exist?*/

/* Step 0: Reset and start a new CAS session */
cas mySession terminate;                     /* End any existing session just in case */
cas mySession;                               /* Start a new CAS session */
libname mycas cas caslib="casuser";          /* Assign CAS libref to access in-memory CAS datasets */

/* Step 1: Create a list of all nodes (from and to columns) */
data nodes;
    set A3IMPORT(keep=from to);              /* Only keep the from and to columns */
    node = from; output;                     /* Treat 'from' as a node */
    node = to; output;                       /* Treat 'to' as a node */
run;

proc sort data=nodes nodupkey;
    by node;                                 /* Remove duplicate nodes to get a unique list */
run;

/* Step 2: Load datasets into CAS (in-memory processing) */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace;  /* Load the imported list */
    load data=nodes outcaslib="casuser" casout="nodes" replace;        /* Load the unique nodes */
run;

/* Step 3: Use PROC NETWORK to find articulation points */
proc network
    nodes=mycas.nodes                        /* Use the CAS version of the nodes table */
    links=mycas.A3IMPORT                     /* Use the CAS version of the imported list */
    outnodes=mycas.out_nodes                 /* Save node results in CAS (including articulation point flags) */
    outlinks=mycas.out_links;                /* Save link results in CAS */
    biconnectedcomponents;                   /* Analyze the network to find articulation points */
run;

/* Q6 - Which node has the largest capacity to facilitate communicating the fraud, and how many degrees (most adjacent direct connections?*/

/* Step 1: Combine both 'from' and 'to' columns into a single list of nodes */
data all_links;
    set A3IMPORT(keep=from to);     /* Only use the columns that define connections */
    node = from; output;            /* Save the 'from' node as one record */
    node = to; output;              /* Save the 'to' node as one record */
run;

/* Step 2: Count how many times each node appears in the network */
proc freq data=all_links noprint;  
    tables node / out=node_degrees(rename=(count=degree)); /* Count = degree of the node */
run;

/* Step 3: Sort the nodes so the one with the highest degree comes first */
proc sort data=node_degrees out=top_node;
    by descending degree;           /* Sort from highest degree to lowest */
run;

/* Q7 - Which node has the highest prestige using direct connections (cntr_influence_1wt)? */

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Run network analysis to calculate influence centrality (prestige based on direct links) */
proc network
	direction = directed
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
    centrality influence = weight;   /* Calculate influence centrality using weights */
run;

/* Step 5: Sort the results so the node with the highest influence appears first */
proc sort data=mycas.out_nodes out=ranked_direct_prestige;
    by descending centr_influence1_wt;
run;

/* Q8 - Which node has the highest prestige using indirect connections (cntr_influence_2wt)?*/

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Run network analysis to calculate influence centrality (prestige based on indirect links) */
proc network
    direction = undirected
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
    centrality influence = weight;   /* Calculate influence centrality using weights */
run;

/* Step 5: Sort the results so the node with the highest influence appears first */
proc sort data=mycas.out_nodes out=ranked_indirect_prestige;
    by descending centr_influence2_wt;
run;

/* Q9 - Which node is most central based upon closeness (centr_close_wt)? */

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Compute closeness centrality (based on direct connections / shortest paths) */
proc network
    direction = directed
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
    centrality close=weight;   /* Calculate closeness centrality based on weights*/
run;

/* Step 5: Sort by closeness centrality */
proc sort data=mycas.out_nodes out=ranked_by_closeness;
    by descending centr_close_wt;
run;

/* Q10 - Which node is most influential in terms of betweenness? */

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Compute betweenness centrality based on direct connections */
proc network
    direction = directed
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
    centrality between=weight;   /* Calculate betweenness centrality based on weights*/
run;

/* Step 5: Sort by betweenness centrality */
proc sort data=mycas.out_nodes out=ranked_by_betweenness;
    by descending centr_between_wt;
run;

/* Q11 - How many cliques are there? */

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Compute the number of cliques based on undirected graph in a clique without setting a minimum size*/
proc network
    direction=undirected
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
    clique maxcliques=all
	out=mycas.CliquesOut;   
run; 

/*Q12 - How many communities are there?*/

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Runs the algorithm PARALLELLABELPROP for community detection specified in the resolutionList option */
proc network
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
	community ALGORITHM=PARALLELLABELPROP
	resolutionList = 1.0 0.5
   	outLevel=mycas.commLevelOut;
run;

/*Q13 - How many nodes are in the largest community? */

/* Step 0: Start a new clean session for analytics */
cas mySession terminate;     /* Close any existing session if running */
cas mySession;               /* Start a new session */
libname mycas cas caslib="casuser";  /* Set up a shortcut (libref) to load data into memory */

/* Step 1: Create a list of all nodes from the 'from' and 'to' columns */
data nodes;
    set A3IMPORT(keep=from to);      /* Only use the 'from' and 'to' columns from the data */
    node = from; output;             /* Save each 'from' value as a node */
    node = to; output;               /* Save each 'to' value as a node */
run;

/* Step 2: Keep only unique nodes (no duplicates) */
proc sort data=nodes nodupkey;
    by node;
run;

/* Step 3: Load both tables into CAS memory for analysis */
proc casutil;
    load data=A3IMPORT outcaslib="casuser" casout="A3IMPORT" replace; /* Load the link data */
    load data=nodes outcaslib="casuser" casout="nodes" replace;       /* Load the unique nodes */
run;

/* Step 4: Runs the algorithm PARALLELLABELPROP for community detection specified in the resolutionList option */
proc network
    nodes=mycas.nodes
    links=mycas.A3IMPORT
    outnodes=mycas.out_nodes
    outlinks=mycas.out_links;
	community ALGORITHM=PARALLELLABELPROP
	resolutionList = 1.0 0.5
   	outLevel=mycas.commLevelOut;
run;

proc sql;  
    
    create table community_counts as  
    /* Create a new table called community_counts to store the results */
    
    select community_2 as community, count(*) as node_count  
    /* From the input table, select each unique community (from column community_2),
       and count how many nodes are in each one. Rename the count as node_count. */
    
    from mycas.out_nodes  
    /* Use the table out_nodes from memory (CAS) that contains the node and community data */
    
    group by community_2  
    /* Group the data by each community so we can count nodes within each group */
    
    order by node_count desc;  
    /* Sort the results so the largest community (with most nodes) is shown first */
        
quit;  




 