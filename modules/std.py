import sys  


original_stderr = sys.stderr
original_stdout = sys.sydout 

# Redirect stdout and stderr to the terminal
sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__

# Call your C extension function (that writes to stdout/stderr)
#import your_c_extension
#your_c_extension.some_function()

# Restore original stdout and stderr if needed
#sys.stdout = original_stdout
#sys.stderr = original_stderr
