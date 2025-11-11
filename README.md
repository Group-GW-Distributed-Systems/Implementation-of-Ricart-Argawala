# Implementation of Ricart Argawala
Project ITU - Distributed Systems

*How to Run*


*Open Terminal 1 (Node 1)*

Run the client: go run client/main.go

Enter its own port: Type port address to listen at: 5001

Enter the other ports: Type other ports to connect to: 5002 5003 5004


*Open Terminal 2 (Node 2)*

Run the client: go run client/main.go

Enter its own port: Type port address to listen at: 5002

Enter the other ports: Type other ports to connect to: 5001 5003 5004


*Open Terminal 3 (Node 3)*

Run the client: go run client/main.go

Enter its own port: Type port address to listen at: 5003

Enter the other ports: Type other ports to connect to: 5001 5002 5004


*Open Terminal 4 (Node 4)*

Run the client: go run client/main.go

Enter its own port: Type port address to listen at: 5004

Enter the other ports: Type other ports to connect to: 5001 5002 5003

The nodes are now connected.


*Using the Program*

req: Request access to the Critical Section.

exit: Shut down the node.
