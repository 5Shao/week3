import socket
import platform

def get_machine_ip():
    # Get the hostname
    hostname = socket.gethostname()
    # Get the IP address
    ip_address = socket.gethostbyname(hostname)
    return ip_address

def get_machine_name():
    # Get the machine name using platform
    machine_name = platform.node()
    return machine_name

if __name__ == "__main__":
    ip = get_machine_ip()
    name = get_machine_name()
    print(f"Machine IP: {ip}")
    print(f"Machine Name: {name}")
    print("My name is Scott")