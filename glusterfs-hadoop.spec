%define _libdir /usr/local/lib

%define __mkdir_p mkdir -p
%define __rm rm
%define __cp cp

# Plugin supported Hadoop Version
%define _hadoop_ver 0.20.2
%define _plugin_ver 0.1

%define _gluster_core_version 3.3
%define _gluster_core_release 0

# default install prefix
Prefix: %{_libdir}

Summary: GlusterFS Hadoop Plugin
Name: glusterfs
Version: %{_hadoop_ver}
Release: %{_plugin_ver}
Group: Application/File
Vendor: Gluster Inc.
Packager: gluster-users@gluster.org
License: Apache

%description
This plugin provides a transparent layer for Hadoop to run Map/Reduce
jobs on files/data present of GlusterFS.

%package hadoop
Summary: Hadoop Plugin for GlusterFS
Group: Application/File
Requires: glusterfs-core >= %{_gluster_core_version}-%{_gluster_core_release}

%description hadoop
This plugin provides a transparent layer for Hadoop to run Map/Reduce
jobs on files/data present of GlusterFS.

%install
%{__rm} -rf %{buildroot}
%{__mkdir_p} %{buildroot}/%{_libdir}/conf
%{__cp} %{_libdir}/glusterfs-%{_hadoop_ver}-%{_plugin_ver}.jar %{buildroot}/%{_libdir}
%{__cp} -R %{_libdir}/conf/* %{buildroot}/%{_libdir}/conf/

%files hadoop
%defattr(-,root,root)
%{_libdir}/glusterfs-%{_hadoop_ver}-%{_plugin_ver}.jar
%{_libdir}/conf/core-site.xml

%clean
%{__rm} -f %{_libdir}/glusterfs-%{_hadoop_ver}-%{_plugin_ver}.jar
%{__rm} -rf %{_libdir}/conf

%post hadoop
echo ""
echo "====================================================================="
echo "Plugin Files installed in the installed prefix."
echo "Create soft links from hadoop lib/ and conf/ directory to these files."
echo "====================================================================="
echo ""