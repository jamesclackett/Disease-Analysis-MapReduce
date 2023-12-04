from flask import Flask, render_template

app = Flask(__name__)


@app.route('/')
def home():
    # A simple home page controller. Reads a file and loads a jinja template.
    # Could do with much more error handling but that is outside the scope of assignment.
    path = 'merged-output.txt' 
    disease_data = read_output_file(path)

    return render_template('index.html', disease_data=disease_data)


# Reads the map reducd output file.
# Creates a dictionary of disease_name : [ topword1, topword2, ... topwordN ]
def read_output_file(path):

    disease_data = {}

    with open(path, 'r') as f:

        for line in f:
            tokens = line.strip().split('\t')
            if len(tokens) == 2:
                # get disease name from start of line
                disease_name = tokens[0]
                # get the top words associated with the disease.
                top_words = tokens[1].split(',')
                # remove the word count from [count:word]
                words = [pair.split(':')[1] for pair in top_words if len(pair.split(':')) == 2]
                # Put in dictionary
                disease_data[disease_name] = words

    return disease_data

if __name__ == '__main__':
    app.run(debug=False)
