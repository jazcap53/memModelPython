# file: make_junk_in_python.py
# 2024-04-28

def read_omitting_personal(open_file_handle):
    lines = []
    for line in open_file_handle.readlines():
        if 'archo' in line:
            continue
        lines += line
    return lines


output_pre = [
    'make_junk/mem_program_description_too_long.txt',
]

output_body = [
    'ajCrc.py',
    'ajTypes.py',
    'ajUtils.py',
    'arrBit.py',
    'change.py',
    'client.py',
    'crashChk.py',
    'driver.py',
    'fileMan.py',
    'fileShifter.py',
    'freeList.py',
    'inodeTable.py',
    'journal.py',
    'memoryMain.py',
    'memMan.py',
    'myMemory.py',
    'pageTable.py',
    'simDisk.py',
    'status.py',
    'wipeList.py',
    # 'check_new_functionality.py',
    'run_modules.py',
]

output_post = [
    'make_junk/start_file.txt',
    # 'make_junk/end_file.txt',
    # 'make_junk/output_preamble.txt',
    # 'make_junk/cli_output.txt',
    # 'make_junk/question_for_assistant.txt',
    'make_junk/describe_utilities.txt',
    'make_junk/mem_ask_ai_to_do_stuff.txt',
]

output_file_contents = []
with open('make_junk/mem_junk.txt', 'w') as outfile:
    # write pre files
    for file in output_pre:
        with open(file) as current_infile:
            output_file_contents += read_omitting_personal(current_infile)
    # write body files
    for ix, file in enumerate(output_body):
        if ix == len(output_body) - 1:
            disclaimer = '\n\nThis file runs each module (except for memoryMain.py) checking the output'
            output_file_contents += disclaimer
        output_file_contents += f'\n\n{file}:\n'
        output_file_contents += '\n```python\n'
        with open(file) as current_infile:
            output_file_contents += read_omitting_personal(current_infile)
        output_file_contents += '\n```\n'
        output_file_contents += '\n================================\n'

    # write post files
    for file in output_post:
        with open(file) as current_infile:
            output_file_contents += read_omitting_personal(current_infile)

    output_file_contents = ''.join(item for item in output_file_contents)
    outfile.write(output_file_contents)
