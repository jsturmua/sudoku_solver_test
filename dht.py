import socket
import threading
import pickle
import time
from sudoku import Sudoku
from new_solver import solve_sudoku, find_next_empty, is_valid
import selectors

class DHTNode(threading.Thread):
    def __init__(self, host, port, neighbors=None, predecessor=None):
        super().__init__()
        self.starttime = time.time()
        self.host = host
        self.port = port
        self.predecessor = predecessor
        self.neighbors = neighbors if neighbors else []
        self.neighborfree = False
        self.selector = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(5)
        self.sock.setblocking(False)
        self.selector.register(self.sock, selectors.EVENT_READ, self.handle_request)
        self.running = True

    def send_data(self, data, addr):
        """Send data to another node."""
        self.sock.sendto(pickle.dumps(data), addr)

    def receive_data(self):
        """Receive data from other nodes."""
        print("I try to receive data")
        try:
            data, addr = self.sock.recvfrom(1024)
            return pickle.loads(data), addr
        except socket.timeout:
            return None, None
        # try:
        #     data, addr = sock.recvfrom(1024)
        #     if data:
        #         data = pickle.loads(data)
        #         self.handle_request(data, addr)
        # except socket.error as e:
        #     print(f"Socket error: {e}")
        # except pickle.PickleError as pe:
        #     print(f"Pickle error: {pe}")

    def run(self):
        """Main loop to receive and handle data."""
        # while self.running:
        #     data, addr = self.receive_data()
        #     if data:
        #         self.handle_request(data, addr)
        while self.running:
            events = self.selector.select(timeout=None)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

    def non_blocking_receive(self):
        try:
            data, addr = self.sock.recvfrom(1024)  # Assuming the socket is set to non-blocking
            return pickle.loads(data), addr
        except socket.error as e:
            if e.errno != socket.errno.EWOULDBLOCK:
                print(f"Socket error: {e}")
            return None, None  # No data received


    def stop(self):
        """Stop the node."""
        print(f"Node {self.port} stops now")
        self.running = False
        self.selector.unregister(self.sock)
        self.sock.close()
        self.selector.close()

    def handle_request(self, sock, mask):
        data, addr = self.receive_data()
        if data:
            if data['method'] == 'TASK':
                start_time = time.time()
                possible_solutions = data['range']
                # Split the task
                self.sudoku = data['sudoku']

                self.perform_solving(self.sudoku, possible_solutions)
                end_time = time.time()  # End timing after the algorithm completes
                elapsed_time = end_time - start_time  # Calculate the elapsed time
                print(f"Time taken to solve the task for Node {self.port}: {elapsed_time:.2f} seconds")
                print(f"Node {self.port} asks for work")
                self.send_data({
                'method': 'NEEDWORK',
                }, self.predecessor)
                

            elif data['method'] == 'NEEDWORK':
                self.neighborfree = True
                print(f"Node {self.port} got it´s neighbor free")

            elif data['method'] == 'STOP':
                self.stop()

    def perform_counting(self, start, end):
        """Counts from start to end and prints each number."""
        for i in range(start, end + 1):
            print(f"Node {self.port} counting: {i}")
            time.sleep(1)  # Simulate time taken for counting

    def perform_solving(self, grid, arr):
        print(f"Node {self.port} starts solving with range: {arr}")
        solution = self.solve_sudoku(grid, arr)
        print(f"Node {self.port} found solution: {solution}")
        if solution == True:
            print(grid)
            end_time = time.time()  # End timing after the algorithm completes
            elapsed_time = end_time - self.starttime  # Calculate the elapsed time
            print(f"Calculated time for solving: {elapsed_time}")

    
    def solve_sudoku(self, puzzle, arr=range(1, 10)):
        # solve sudoku using backtracking!
        # our puzzle is a list of lists, where each inner list is a row in our sudoku puzzle
        # return whether a solution exists
        # mutates puzzle to be the solution (if solution exists)
        # Check if neighbor is free and give him work
        message, addr = self.non_blocking_receive()
        if message:
            # Handle the message
            print(f"Received message: {message} from {addr}")
            if message['method'] == 'NEEDWORK':
                self.neighborfree = True
                print(f"Node {self.port} got his neighbor free")
        if self.neighbors and self.neighborfree:
            print(f"Node {self.port} sends half to {self.neighbors[0]}")
            # Delegate the rest to the neighbor
            first_half, arr = split_array_in_middle(arr)
            self.send_data({
                'method': 'TASK',
                'sudoku': self.sudoku,
                'range': first_half
            }, self.neighbors[0])
            self.neighborfree = False
            #self.perform_counting(start_point, end_point)
            print(f"Node {self.port} checks with range: {arr}")


        # step 1: choose somewhere on the puzzle to make a guess
        row, col = find_next_empty(puzzle)

        # step 1.1: if there's nowhere left, then we're done because we only allowed valid inputs
        if row is None:  # this is true if our find_next_empty function returns None, None
            return True 
        
        # step 2: if there is a place to put a number, then make a guess between 1 and 9
        for guess in arr: # range(1, 10) is 1, 2, 3, ... 9
            time.sleep(0.01)
            # step 3: check if this is a valid guess
            if is_valid(puzzle, guess, row, col):
                # step 3.1: if this is a valid guess, then place it at that spot on the puzzle
                puzzle[row][col] = guess
                # step 4: then we recursively call our solver!
                if self.solve_sudoku(puzzle):
                    return True
            
            # step 5: it not valid or if nothing gets returned true, then we need to backtrack and try a new number
            puzzle[row][col] = 0

        # step 6: if none of the numbers that we try work, then this puzzle is UNSOLVABLE!!
        return False
        
def split_array_in_middle(arr):
    # Find the middle index
    middle_index = len(arr) // 2

    # Split the array into two halves
    first_half = arr[:middle_index]
    second_half = arr[middle_index:]

    return first_half, second_half

def initiate_distributed_task_3_nodes(sudoku):
    # Set up two DHT nodes
    node1 = DHTNode('localhost', 5000, [('localhost', 5001)], ('localhost', 5002))
    node2 = DHTNode('localhost', 5001,  [('localhost', 5002)], ('localhost', 5000))
    node3 = DHTNode('localhost', 5002,  [('localhost', 5000)], ('localhost', 5001))


    # Set up one DHT node
    #node1 = DHTNode('localhost', 5000)

    # Start nodes
    node1.start()
    node2.start()
    node3.start()


    # Node 1 receives a command to count to 10
    #node1.send_data({'method': 'TASK', 'range': (1, 10)}, ('localhost', 5000))

    #Node 1 receives the sudoku
    node1.send_data({'method': 'NEEDWORK'}, ('localhost', 5000))
    node2.send_data({'method': 'NEEDWORK'}, ('localhost', 5001))
    node1.send_data({'method': 'TASK','sudoku': sudoku.grid, 'range': range(1, 10)}, ('localhost', 5000))

    # Wait for the nodes to finish
    time.sleep(100)

    # Stop nodes
    print("I send the stop now")
    node1.send_data({'method': 'STOP'}, ('localhost', 5000))
    node2.send_data({'method': 'STOP'}, ('localhost', 5001))
    node3.send_data({'method': 'STOP'}, ('localhost', 5002))
    # node1.stop()
    # node2.stop()
    node1.join()
    node2.join()
    node3.join()


def initiate_distributed_task_2_nodes(sudoku):
    # Set up two DHT nodes
    node1 = DHTNode('localhost', 5000, [('localhost', 5001)], ('localhost', 5001))
    node2 = DHTNode('localhost', 5001,  [('localhost', 5000)], ('localhost', 5000))


    # Set up one DHT node
    #node1 = DHTNode('localhost', 5000)

    # Start nodes
    node1.start()
    node2.start()


    # Node 1 receives a command to count to 10
    #node1.send_data({'method': 'TASK', 'range': (1, 10)}, ('localhost', 5000))

    #Node 1 receives the sudoku
    node1.send_data({'method': 'NEEDWORK'}, ('localhost', 5000))
    node1.send_data({'method': 'TASK','sudoku': sudoku.grid, 'range': range(1, 10)}, ('localhost', 5000))

    # Wait for the nodes to finish
    time.sleep(100)

    # Stop nodes
    print("I send the stop now")
    node1.send_data({'method': 'STOP'}, ('localhost', 5000))
    node2.send_data({'method': 'STOP'}, ('localhost', 5001))
    # node1.stop()
    # node2.stop()
    node1.join()
    node2.join()


if __name__ == "__main__":
    sudoku = Sudoku([
        [5, 3, 0, 0, 7, 0, 0, 0, 0],
        [6, 0, 0, 1, 9, 5, 0, 0, 0],
        [0, 9, 8, 0, 0, 0, 0, 6, 0],
        [8, 0, 0, 0, 6, 0, 0, 0, 3],
        [4, 0, 0, 8, 0, 3, 0, 0, 1],
        [7, 0, 0, 0, 2, 0, 0, 0, 6],
        [0, 6, 0, 0, 0, 0, 2, 8, 0],
        [0, 0, 0, 4, 1, 9, 0, 0, 5],
        [0, 0, 0, 0, 8, 0, 0, 7, 9]
    ])
    initiate_distributed_task_2_nodes(sudoku)

    