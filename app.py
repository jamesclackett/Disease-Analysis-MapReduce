from flask import Flask, render_template

app = Flask(__name__)

def read_file(file_path):
    disease_data = {}
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split('\t')
            if len(parts) == 2:
                disease_name = parts[0]
                word_count_pairs = parts[1].split(',')
                words = [pair.split(':')[1] for pair in word_count_pairs if len(pair.split(':')) == 2]
                disease_data[disease_name] = words

    return disease_data

@app.route('/')
def index():
    file_path = 'merged-output.txt'  # Replace with the actual file path
    disease_data = read_file(file_path)
    return render_template('index.html', disease_data=disease_data)

if __name__ == '__main__':
    app.run(debug=True)
