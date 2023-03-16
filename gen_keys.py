import rsa
import yaml


def main():
    path = "config.yaml"
    with open(path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    for client in config['clients']:
        public_path = "secrets/node_" + str(client['nodeId']) + "_public_key.pem"
        private_path = "secrets/node_" + str(client['nodeId']) + "_private_key.pem"

        (public_key, private_key) = rsa.newkeys(2048)
        with open(public_path, "wb") as public_file:
            public_file.write(public_key.save_pkcs1())

        with open(private_path, "wb") as private_file:
            private_file.write(private_key.save_pkcs1())


if __name__ == '__main__':
    main()