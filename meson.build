# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

project('pg-copy-arrow', 'c', 'cpp',
        default_options: [
          'buildtype=debug',
          'cpp_std=c++17',
        ],
        license: 'Apache-2.0',
        version: '0.0.1')

fs = import('fs')
python = import('python')
python3 = python.find_installation('python3')

prefix = get_option('prefix')
data_dir = prefix / get_option('datadir')
doc_dir = data_dir / 'doc'
project_doc_dir = doc_dir / meson.project_name()

postgresql_dir = get_option('postgresql_dir')
pg_config_names = []
if postgresql_dir != ''
  pg_config_names += [postgresql_dir / 'bin' / 'pg_config']
endif
pg_config_names += ['pg_config']
pg_config = find_program(pg_config_names, version: '>= 15')

cpp_args = []
arrow = dependency('Arrow', method: 'cmake', modules: ['Arrow::arrow_shared'])
postgresql = declare_dependency(
  compile_args: run_command(pg_config,
                            '--cflags_sl',
                            check: true).stdout().strip(),
  include_directories: [
    include_directories(
      run_command(pg_config,
                  '--includedir-server',
                  check: true).stdout().strip(),
      is_system: true
    ),
  ]
)
lib_dir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
name_suffix = 'so'
if host_machine.system() == 'darwin' and \
   pg_config.version().version_compare('>= 16')
  name_suffix = 'dylib'
endif
afs = shared_module(
  'copy_arrow',
  'copy_arrow.cc',
  dependencies: [arrow, postgresql],
  name_prefix: '',
  name_suffix: name_suffix,
  install: true,
  install_dir: lib_dir,
)

share_dir = run_command(pg_config, '--sharedir', check: true).stdout().strip()
install_data(
  'copy_arrow.control',
  'copy_arrow--0.0.1.sql',
  install_dir: share_dir / 'extension'
)

install_data(
  'LICENSE.txt',
  'NOTICE.txt',
  'README.md',
  install_dir: project_doc_dir,
)

result_dirs = [
  'results' / 'copy_to_arrow',
  'results' / 'scan_to_arrow',
]
ensure_results = custom_target('ensure-results',
                               command: ['mkdir', '-p'] + result_dirs,
                               output: 'results')
bin_dir = run_command(pg_config, '--bindir', check: true).stdout().strip()
pgxs_mk = run_command(pg_config, '--pgxs', check: true).stdout().strip()
pgxs_src = fs.parent(fs.parent(pgxs_mk))
pg_regress = find_program(pgxs_src / 'test' / 'regress' / 'pg_regress')
test('regress',
     pg_regress,
     args: [
       '--bindir', bin_dir,
       '--inputdir', meson.current_source_dir(),
       '--load-extension', 'copy_arrow',
       'copy_to_arrow' / 'int32',
       'scan_to_arrow' / 'int32',
     ],
     depends: [ensure_results],
     protocol: 'tap')
