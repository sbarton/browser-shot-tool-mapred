browser-shot-tool-mapred
========================
The browser-shots tool is developed by Internet Memory in the context of SCAPE project, for the preservation and watch (PW) sub-project. The goal of this tool is to perform automatic visual comparisons, in order to detect rendering issues in the archived Web pages and report it to SCOUT via C3PO. 

The detection of the rendering issues is done in the following three steps :  
  - Web pages screenshots automatically taken using Selenium framework, for different browser versions.
  - Visual comparison between pairs of screenshots using MarcAlizer tool (recently replaced by PageAlizer tool, to include also the structural comparison).
  - Automatically detect the rendering issues in the Web pages, based on the comparison results.

The tool represents a mapreduce job, tak takes as an input a list of URLs and a list of web browsers (names), and outputs for each page a similarity score - the result of a comparison of the web page rendered using Selenium and given two browsers. The output is a zipped set of XML files each containing one comparison result.

The browser versions currently experienced and tested are: Firefox (for all the available releases) and Opera (for the official 11th and 12th versions).

Besides running Selenium a hadoop environemnt needs to be installed. The sets of machines running selenium and hadoop can be totally separated - which is in fact recommended to get better performance results. This repo contains a maven project where currently Hadoop 2.0 is set as a dependency. It uses quite common APIs so it should work with other distributions as well. 

###1. Getting Started :

The deployment and installation of the browser-shots tool is strongly dependent on different other packages, since it uses "off-the-shelf" components that need to be already available on your system, such as:
 - Selenium 2.24.1
 - Firefox 3.6
 - Hadoop Distribution: HDFS, MapReduce (Cloudera CDH4 recommended)

In order to make all the tools running together, in a suitable environment, the following applications/packages need to be installed :

  **1.1.** Selenium driver for the browsers : provided by Selenium in the Python Client on its official website (for example, the driver for Firefox is used in this project)
Reference : http://pypi.python.org/pypi/selenium

  **1.2.** If the Graphical User Interface (GUI) is not available in your system, you can use an X server (for example, we used Xvfb v 11)
Packages to be installed: xvfb, xfonts-base, xfonts-75dpi, xfonts-100dpi, libgl1-mesa-dri, xfonts-scalable, xfonts-cyrillic, gnome-icon-theme-symbolic

  **1.3.** How to install a Hadoop environment can we found here: http://www.cloudera.com/content/support/en/documentation/cdh4-documentation/cdh4-documentation-v4-latest.html



###2. Testing and running the main building blocks :

**2.1. Selenium**  
The Selenium framework can be used in two different cases: the first one is to run it on the local machine and the second one is to run Selenium as a server and several nodes in a distributed solution. The nodes will be called remotely by the server.
To explore all options in the jar file type :

    $ java -jar selenium-server-standalone-2.24.1.jar -h
To configure Selenium in distributed architecture and establish a connection with different nodes :
Run a selenium hub using the option "-role" :

    $ java -jar selenium-server-standalone-2.24.1.jar -port 8089 -role hub
And then, from the same Selenium jar, run the rest as selenium nodes by precising the option "-role" as "node" and the option "-hub" the address of the hub, following the example given above, the hub address is "http://machine-hub.com:8089/grid/register" :

    $ java -jar selenium-server-standalone-2.24.1.jar -role node -port 5555 -hub http://machine-hub.com:8089/grid/register -maxSession 10 -browser browserName=firefox,maxInstances=10 -host http://machine-node.com
    
The option "-browser" define the browser that will open pages through the port 5555 and the max instances of the browser at the ame time.
If the GUI is not installed in your system, you have to launch all the nodes of selenium in a virtual screen (the server X) :

    $ Xvfb :1 -screen 0 1024x768x24 &
    $ DISPLAY=:1 java -jar selenium-server-standalone-2.24.1.jar -role node -port 5555 -hub http://machine-hub.com:8089/grid/register -maxSession 10 -browser browserName=firefox,maxInstances=10 -host http://machine-node.com/

**2.2. Firefox**  
In order to use Firefox, one option is to create a symbolic link to Firefox and then run the firefox command :

    $ firefox http://google.com

If you don't have a GUI, an error will be displayed: "Error: no display specified".  
If this is the case, you need to run the Xserver first :

    $ Xvfb :1 -screen 0 1024x768x24 &
    $ DISPLAY=:1 firefox http://google.com

###3. Running the job :



**3.1. Build the maven project**  

    $ cd <project_base_dir>
    $ mvn package

This command will build the jar we use to run the job, but also creates a lib dir where it puts all the dependencies, that need to be present (and on the hadoop class path) of each workder node in the Hadoop cluster (worker node is a node running TaskTracker deamon).

    $ ls  <project_base_dir>/target/Browsershots-0.9-SNAPSHOT-bin/Browsershots-0.9-SNAPSHOT/lib/

	
**3.2. Run the job**  

The parameters of job can be acuired by running following command:
    $ hadoop jar <path_to_the_project_jar>/<project.jar> net.internetmemory.browsershots.PageRenderCompareMapRed -help

    $ usage: PageRenderCompareMapRed
    $ -browser <browsers>        list of browsers, at least two, e.g. -browser
    $                            b1 -browser b2
    $ -definition <definition>   marcalizer definition file path
    $ -help                      print this message
    $ -input <Input path>        Path input
    $ -output <output path>      Path output


Invoking the command using the hadoop command ensures, that necessary hadoop dependencies are set on the java's class path. The order of the mentioned browsers does matter, where the first is the reference browser - all other snapshots are compared to this one.

Parameter -definition denotes the XML representation of the Marcalizer configuration. Example file can be found in the project sources (src/main/example). This file needs to be present at each worker machine on the specified path.

**3.3. Collecting the results**  
The results can be found in the HDFS directory denoted as -output parameter. The results contains a zip file containing an XML file for each pair of comparisons.
