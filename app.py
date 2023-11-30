from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST': 
        # get data from input fields
        st_var_mean = request.form.get("st_var_mean")
        perc_time_abnormal_st_var = request.form.get("perc_time_abnormal_st_var")
        abnormal_st_var = request.form.get("abnormal_st_var")
        hist_mean = request.form.get("hist_mean")
        hist_var = request.form.get("hist_var")

        # Run the model with the input fields (for now just return dummy %)

        print("inputs: ", st_var_mean, perc_time_abnormal_st_var, abnormal_st_var, hist_mean, hist_var)

        result = 42

        return render_template('result.html', result=result)
    
    return render_template('home.html')

if __name__ == '__main__':
    app.run(debug=True)