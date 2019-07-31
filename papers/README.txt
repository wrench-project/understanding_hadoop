Current Simulators:
------------------
    - MSRG ("MSRG - A MapReduce Simulator Over SimGrid")
        - task costs are described through a function 
        - "all steps, exclusding the data transfers, are considered to be embedded 
            in the SimGrid MFlops returned by the task-cost user function"
        - amount of data generated from a mapper that is received by a reducer 
            is given through a user defined function 

    - MRPerf ("A Simulation Approach to Evaluating Design Decisions
            in MapReduce Setups")
        - lacks a number of Hadoop MapReduce job configuration parameters
        - uses DiskSim to model disk usage
        - uses ns-2 to model network usage 
        - map and reduce task computation times are set based on a "cycles/byte" parameter

    - SimMR ("Play It Again, SimMR!")
        - replays execution traces obtained from real world workloads 
            (or a synthetic workload can be generated)
        - not aimed at simulating lower level details of map/reduce tasks and
            therefore cannot simulate changes in certain configuration parameters 

    - MRSim ("MRSim: A Discreet Event Based MapReduce Simulator")
        - input records are given some average size
        - considers all of the configuration parameters we have looked at in the tests,
            however we do not know how accurately changes in those properties are simulated
        - network topology and traffic simulated with GridSim
        - other system entities are modeled using SimJava discrete event engine 

Performance Modeling:
--------------------
    - "An Analytical Performance Model of MapReduce"
        - develops a model that focuses on the relationship between the overall performance and 
            number of mappers and reducers
        - finds that running times of mappers and reducers is a function of their input sizes
        - finds that although the number of mappers is linear in the data size, this is not optimal 
        - see section 3: Performance model  

    - "On the Performance of MapReduce: A Stochastic Approach"
        - may have useful models regarding input data and I/O, however focuses on
            a shared-memory MapReduce where a single host performas all MapReduce jobs.. 
    - "The Case for Evaluating MapReduce Performance Using Workload Suites"
        - suggests going beyond the use of benchmarks for performance evaluations by using
            real world workloads instead 
